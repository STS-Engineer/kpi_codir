require("dotenv").config();
const express = require("express");
const { Pool } = require("pg");
const bodyParser = require("body-parser");
const cors = require("cors");
const nodemailer = require("nodemailer");
const cron = require("node-cron");
const { registerRecommendationRoutes } = require('./kpi-recommendations');
const { generateKPIRecommendationsPDFBuffer, generatePlantKPIRecommendationsPDFBuffer } = require('./kpi-recommendations');
const {
  buildDelayKnowledgeBaseContext
} = require("./delay-knowledge-base");
const app = express();
const port = process.env.PORT || 5000;

app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// ---------- Postgres ----------
const pool = new Pool({
  user: "administrationSTS",
  host: "avo-adb-002.postgres.database.azure.com",
  database: "kpi_codir_test",
  password: "St$@0987",
  port: 5432,
  ssl: { rejectUnauthorized: false },
});


cron.schedule('34 09 * * 1', async () => {
  console.log(`[CRON] Running KPI week update — ${new Date().toISOString()}`);
  try {
    await pool.query('SELECT public.update_kpi_week()');
    console.log('[CRON] ✅ kpi_values.week updated successfully');
  } catch (err) {
    console.error('[CRON] ❌ Failed to update kpi_values.week:', err.message);
  }
}, {
  timezone: 'Africa/Tunis'   // ← ensures 14:00 Tunis local time
});

// ============================================================
// KPI DIRECTION + STATUS HELPERS
// up   = higher is better
// down = lower is better
// ============================================================
const KPI_DIRECTION_OVERRIDES = Object.freeze({
  // Add explicit overrides here if a KPI direction should not be inferred.
  // Example: 11: "down"
});

const DOWN_DIRECTION_PATTERNS = [
  /\bclaims?\b/i,
  /\bbacklog\b/i,
  /\bstock\b/i,
  /\binventory\b/i,
  /\bcosts?\b/i,
  /\bfees?\b/i,
  /\bmore than\b/i,
  /\bdelay\b/i,
  /\blate\b/i,
];

const parseMetricNumber = (value) => {
  if (value === null || value === undefined || value === '' || value === 'None') {
    return null;
  }
  const parsed = parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const normalizeKpiDirection = (value) => {
  const text = String(value ?? '').trim().toLowerCase();
  if (!text) return null;

  if (
    text === 'up' ||
    text === 'higher' ||
    text === 'higher is better' ||
    text === 'increase' ||
    text === 'increasing' ||
    text === 'maximize' ||
    text === 'max'
  ) {
    return 'up';
  }

  if (
    text === 'down' ||
    text === 'lower' ||
    text === 'lower is better' ||
    text === 'decrease' ||
    text === 'decreasing' ||
    text === 'minimize' ||
    text === 'min'
  ) {
    return 'down';
  }

  return null;
};

const getKpiBounds = (lowLimit, highLimit) => {
  const low = parseMetricNumber(lowLimit);
  const high = parseMetricNumber(highLimit);
  const bounds = [low, high].filter((value) => value !== null);

  if (!bounds.length) {
    return { lowerBound: null, upperBound: null };
  }

  return {
    lowerBound: Math.min(...bounds),
    upperBound: Math.max(...bounds)
  };
};

const inferKpiDirection = (kpi = {}) => {
  const explicitDirection = normalizeKpiDirection(
    kpi.good_direction ||
    kpi.goodDirection ||
    kpi.direction ||
    kpi.performance_direction
  );
  if (explicitDirection) return explicitDirection;

  const overrideKey = String(kpi.kpi_id ?? kpi.kpiId ?? kpi.id ?? '');
  const overrideDirection = normalizeKpiDirection(KPI_DIRECTION_OVERRIDES[overrideKey]);
  if (overrideDirection) return overrideDirection;

  const { lowerBound, upperBound } = getKpiBounds(
    kpi.low_limit ?? kpi.lowLimit,
    kpi.high_limit ?? kpi.highLimit
  );
  const target = parseMetricNumber(kpi.target);

  if (target !== null && lowerBound !== null && upperBound !== null) {
    if (target <= lowerBound) return 'down';
    if (target >= upperBound) return 'up';
  }

  const searchableText = [
    kpi.subject,
    kpi.title,
    kpi.indicator_sub_title,
    kpi.subtitle
  ]
    .filter(Boolean)
    .join(' ');

  if (DOWN_DIRECTION_PATTERNS.some((pattern) => pattern.test(searchableText))) {
    return 'down';
  }

  return 'up';
};

const getKpiStatus = (value, lowLimit, highLimit, direction = 'up') => {
  const val = parseMetricNumber(value);
  const resolvedDirection = normalizeKpiDirection(direction) || 'up';
  const { lowerBound, upperBound } = getKpiBounds(lowLimit, highLimit);

  if (val === null) {
    return {
      color: '#6c757d',
      isGood: null,
      direction: resolvedDirection,
      lowerBound,
      upperBound
    };
  }

  if (resolvedDirection === 'down') {
    return {
      color: upperBound !== null && val > upperBound ? '#dc3545' : '#28a745',
      isGood: !(upperBound !== null && val > upperBound),
      direction: resolvedDirection,
      lowerBound,
      upperBound
    };
  }

  return {
    color: lowerBound !== null && val < lowerBound ? '#dc3545' : '#28a745',
    isGood: !(lowerBound !== null && val < lowerBound),
    direction: resolvedDirection,
    lowerBound,
    upperBound
  };
};

const needsCorrectiveAction = (value, lowLimit, highLimit, direction = 'up') =>
  getKpiStatus(value, lowLimit, highLimit, direction).isGood === false;

const getDotColor = (value, lowLimit, highLimit, direction = 'up') =>
  getKpiStatus(value, lowLimit, highLimit, direction).color;

// ---------- IMPROVED Job Lock Helper with PostgreSQL Advisory Locks ----------
const acquireJobLock = async (lockId, ttlMinutes = 9) => {
  const instanceId = process.env.WEBSITE_INSTANCE_ID || `instance_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  try {
    const lockHash = Math.abs(lockId.split('').reduce((a, b) => ((a << 5) - a) + b.charCodeAt(0), 0));
    const result = await pool.query('SELECT pg_try_advisory_lock($1) as acquired', [lockHash]);
    if (result.rows[0].acquired) {
      console.log(`🔒 Instance ${instanceId} acquired lock ${lockId}`);
      return { acquired: true, instanceId, lockHash };
    } else {
      return { acquired: false, instanceId, lockHash };
    }
  } catch (error) {
    return { acquired: false, instanceId, error: error.message };
  }
};

const releaseJobLock = async (lockId, lockHash) => {
  try {
    if (lockHash) {
      await pool.query('SELECT pg_advisory_unlock($1)', [lockHash]);
    }
  } catch (error) {
    console.error(`⚠️ Could not release lock ${lockId}:`, error.message);
  }
};

// ---------- Nodemailer ----------
const createTransporter = () =>
  nodemailer.createTransport({
    host: "avocarbon-com.mail.protection.outlook.com",
    port: 25,
    secure: false,
    auth: {
      user: "administration.STS@avocarbon.com",
      pass: "shnlgdyfbcztbhxn",
    },
  });

const getOpenAIClient = () => {
  if (!process.env.SECRET_KEY) {
    throw new Error("OpenAI API key is not configured");
  }

  const OpenAI = require("openai");
  return new OpenAI({ apiKey: process.env.SECRET_KEY });
};

const formatInputDate = (dateValue) => {
  if (!dateValue) return "";
  const d = new Date(dateValue);
  if (isNaN(d.getTime())) return "";
  return d.toISOString().split("T")[0];
};

const normalizeText = (value) => {
  const text = String(value ?? "").trim();
  return text ? text : null;
};

const escapeHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const toArray = (value) => {
  if (Array.isArray(value)) return value;
  if (value === undefined || value === null) return [];
  return [value];
};

const getCorrectiveActionSortTime = (action = {}) => {
  const rawValue =
    action.updated_date ??
    action.updatedDate ??
    action.created_date ??
    action.createdDate ??
    null;

  const timestamp = rawValue ? new Date(rawValue).getTime() : 0;
  return Number.isFinite(timestamp) ? timestamp : 0;
};

const sortCorrectiveActions = (actions = []) =>
  [...actions].sort((a, b) => {
    const timeDiff = getCorrectiveActionSortTime(a) - getCorrectiveActionSortTime(b);
    if (timeDiff !== 0) return timeDiff;

    const idA = parseInt(a.corrective_action_id ?? a.correctiveActionId ?? 0, 10);
    const idB = parseInt(b.corrective_action_id ?? b.correctiveActionId ?? 0, 10);
    return idA - idB;
  });

const getLatestCorrectiveAction = (actions = []) => {
  const sorted = sortCorrectiveActions(actions);
  return sorted.length ? sorted[sorted.length - 1] : null;
};

const hasMeaningfulCorrectiveActionInput = (action = {}) =>
  Boolean(
    normalizeText(action.rootCause ?? action.root_cause) &&
    normalizeText(action.implementedSolution ?? action.implemented_solution) &&
    normalizeText(action.dueDate ?? action.due_date) &&
    normalizeText(action.responsibleName ?? action.responsible)
  );

const getCorrectiveActionStatusValue = ({
  status
}) => normalizeText(status) || "Open";

const readSubmittedFieldList = (formData, fieldName) =>
  toArray(formData?.[`${fieldName}[]`] ?? formData?.[fieldName]);

const getSubmittedCorrectiveActions = (formData, kpiValuesId, defaultResponsibleName = null) => {
  const actionIds = readSubmittedFieldList(formData, `ca_action_id_${kpiValuesId}`);
  const rootCauses = readSubmittedFieldList(formData, `root_cause_${kpiValuesId}`);
  const implementedSolutions = readSubmittedFieldList(formData, `implemented_solution_${kpiValuesId}`);
  const dueDates = readSubmittedFieldList(formData, `due_date_${kpiValuesId}`);
  const responsibleNames = readSubmittedFieldList(formData, `responsible_${kpiValuesId}`);
  const statuses = readSubmittedFieldList(formData, `ca_status_${kpiValuesId}`);

  const actionCount = Math.max(
    actionIds.length,
    rootCauses.length,
    implementedSolutions.length,
    dueDates.length,
    responsibleNames.length,
    statuses.length
  );

  return Array.from({ length: actionCount }, (_, index) => ({
    correctiveActionId: normalizeText(actionIds[index]),
    rootCause: normalizeText(rootCauses[index]),
    implementedSolution: normalizeText(implementedSolutions[index]),
    dueDate: normalizeText(dueDates[index]),
    responsibleName:
      normalizeText(responsibleNames[index]) ||
      normalizeText(defaultResponsibleName),
    status: normalizeText(statuses[index]) || "Open"
  }));
};
const buildCorrectiveActionEntryHtml = ({
  kpiValuesId,
  actionIndex,
  action = {},
  defaultResponsibleName = "",
  showRequired = false
}) => {
  const correctiveActionId = action.corrective_action_id ?? action.correctiveActionId ?? "";
  const rootCause = action.root_cause ?? action.rootCause ?? "";
  const implementedSolution = action.implemented_solution ?? action.implementedSolution ?? "";
  const dueDate = formatInputDate(action.due_date ?? action.dueDate);
  const responsibleName = action.responsible ?? action.responsibleName ?? defaultResponsibleName ?? "";
  const statusText = "Open";
  const safeStatusClass = String(statusText || "")
    .toLowerCase()
    .replace(/\s+/g, "-");
  const requiredAttr = showRequired ? "required" : "";
  const removeButtonClass = correctiveActionId ? "ca-remove-btn is-hidden" : "ca-remove-btn";

  return `
    <div class="ca-action-card" data-existing-action="${correctiveActionId ? "1" : "0"}">
      <input type="hidden" name="ca_action_id_${kpiValuesId}[]" value="${escapeHtml(correctiveActionId)}" />
      <input type="hidden" name="ca_status_${kpiValuesId}[]" value="${escapeHtml(statusText)}" data-ca-field="status" />

      <div class="ca-action-head">
        <div class="ca-action-title">
          Action <span class="ca-action-number">${actionIndex + 1}</span>
        </div>

        <div class="ca-action-tools">
          ${statusText ? `<span class="ca-status-badge ca-status-${safeStatusClass}">${escapeHtml(statusText)}</span>` : ""}
          <button type="button" class="${removeButtonClass}">Remove</button>
        </div>
      </div>

      <div class="ca-dates-grid">
        <div class="ca-field">
          <label class="ca-label">
            Due Date <span class="ca-required">*</span>
          </label>
          <input
            type="date"
            name="due_date_${kpiValuesId}[]"
            class="ca-date-input ca-required-field"
            data-ca-field="due_date"
            value="${escapeHtml(dueDate)}"
            ${requiredAttr}
          />
        </div>

        <div class="ca-field">
          <label class="ca-label">
            Responsible <span class="ca-required">*</span>
          </label>
          <input
            type="text"
            name="responsible_${kpiValuesId}[]"
            class="ca-text-input ca-required-field"
            data-ca-field="responsible"
            value="${escapeHtml(responsibleName)}"
            ${requiredAttr}
          />
        </div>
      </div>

      <div class="ca-field">
        <label class="ca-label">
          Root Cause Analysis <span class="ca-required">*</span>
        </label>
        <textarea
          name="root_cause_${kpiValuesId}[]"
          class="ca-textarea ca-required-field"
          data-ca-field="root_cause"
          placeholder="Describe the root cause..."
          ${requiredAttr}
        >${escapeHtml(rootCause)}</textarea>
      </div>

      <div class="ca-field">
        <label class="ca-label">
          Implemented Solution <span class="ca-required">*</span>
        </label>
        <textarea
          name="implemented_solution_${kpiValuesId}[]"
          class="ca-textarea ca-required-field"
          data-ca-field="implemented_solution"
          placeholder="Describe the implemented solution..."
          ${requiredAttr}
        >${escapeHtml(implementedSolution)}</textarea>
      </div>
    </div>
  `;
};

const buildAssistantKpiContext = (kpis = []) =>
  kpis.slice(0, 30).map((kpi) => ({
    kpi_id: kpi.kpi_id ?? null,
    kpi_values_id: kpi.kpi_values_id ?? null,
    title: normalizeText(kpi.title || kpi.subject) || "Untitled KPI",
    subtitle: normalizeText(kpi.subtitle || kpi.indicator_sub_title),
    unit: normalizeText(kpi.unit),
    current_value: normalizeText(kpi.current_value ?? kpi.value),
    target: normalizeText(kpi.target),
    low_limit: normalizeText(kpi.low_limit),
    high_limit: normalizeText(kpi.high_limit),
    week: normalizeText(kpi.week),
    comment: normalizeText(kpi.comment),
    corrective_action_status: normalizeText(kpi.corrective_action_status || kpi.ca_status),
    root_cause: normalizeText(kpi.root_cause),
    implemented_solution: normalizeText(kpi.implemented_solution),
    evidence: normalizeText(kpi.evidence),
    good_direction: normalizeText(kpi.good_direction) || inferKpiDirection(kpi),
    due_date: normalizeText(kpi.due_date),
    responsible: normalizeText(kpi.responsible),
    corrective_actions: Array.isArray(kpi.corrective_actions)
      ? kpi.corrective_actions.slice(0, 5).map((action) => ({
        corrective_action_id: action.corrective_action_id ?? action.correctiveActionId ?? null,
        root_cause: normalizeText(action.root_cause ?? action.rootCause),
        implemented_solution: normalizeText(action.implemented_solution ?? action.implementedSolution),
        evidence: normalizeText(action.evidence),
        due_date: normalizeText(action.due_date ?? action.dueDate),
        responsible: normalizeText(action.responsible ?? action.responsibleName),
        status: normalizeText(action.status)
      }))
      : []
  }));

const buildAssistantKpiDisplayName = (title, subtitle) => {
  const cleanTitle = normalizeText(title);
  const cleanSubtitle = normalizeText(subtitle);

  if (cleanSubtitle && cleanTitle) {
    return `${cleanSubtitle} (${cleanTitle})`;
  }

  return cleanSubtitle || cleanTitle || "Untitled KPI";
};

const KPI_DELAY_KB_RULES = Object.freeze([
  {
    id: "quote_flow",
    patterns: [/\bquote\b/i, /\bquotation\b/i, /\bdevis\b/i, /\brfq\b/i, /\bcosting\b/i, /\bestimat/i],
    rationale: "This KPI is directly connected to the quotation and costing process, so the delay knowledge base is directly relevant.",
    preferredNodeIds: ["P008", "M001", "P001", "P003", "P007", "P005", "P002", "P006", "S001", "S004", "S006"],
    kbHints: [
      "quote released late",
      "incomplete RFQ package at intake",
      "late supplier quotation",
      "detailed costing blocked",
      "costing rework",
      "quote OTD"
    ],
    ownerFocus: ["Sales", "Costing", "Purchasing", "Management"],
    metricFocus: ["Quote OTD", "Average quote lead time", "Supplier response lead time", "% RFQ complete at intake", "Backlog days"],
    nextChecks: [
      "Check committed quote date versus actual release date",
      "Review whether the RFQ package was complete before costing started",
      "Identify blocked cost lines waiting on supplier or engineering input",
      "Review reprioritization or backlog changes during the quote cycle"
    ],
    nodeRelevanceMap: {
      P008: "This KPI is directly affected when quotes are released after the committed customer date.",
      M001: "Quote OTD is the primary timing metric to verify whether quotation lateness is driving this KPI.",
      P001: "Incomplete RFQ intake delays quote start and creates clarification loops before a customer-ready offer exists.",
      P003: "Late supplier prices delay quote completion when bought parts or material costs are still open.",
      P007: "Blocked detailed costing prevents a complete and reliable commercial release.",
      P005: "Repeated costing rework consumes time and pushes quote release later.",
      P002: "Insufficient costing capacity increases backlog and slows quotation throughput.",
      P006: "Frequent reprioritization interrupts quote completion and causes missed due dates.",
      S001: "An intake gate reduces late starts caused by unstable or missing RFQ inputs.",
      S004: "Should-cost and supplier SLAs reduce waiting time on supplier-dependent cost lines.",
      S006: "Priority governance protects the quote flow from constant interruption."
    }
  },
  {
    id: "commercial_outcome",
    patterns: [/\bbusiness take\b/i, /\border intake\b/i, /\bsales\b/i, /\brevenue\b/i, /\bbookings?\b/i],
    rationale: "Commercial performance can be reduced when quotes are late, incomplete, or blocked before customer release.",
    preferredNodeIds: ["P008", "M001", "P001", "P003", "P002", "P006", "S001", "S004", "S006"],
    kbHints: [
      "quote released late",
      "quote OTD",
      "incomplete RFQ package at intake",
      "late supplier quotation",
      "insufficient costing capacity",
      "shifting priorities and permanent urgencies"
    ],
    ownerFocus: ["Sales", "Purchasing", "Costing", "Management"],
    metricFocus: ["Quote OTD", "% RFQ complete at intake", "Supplier response lead time", "Backlog days", "Priority changes per week"],
    nextChecks: [
      "Review the last 5 missed or delayed business opportunities",
      "Map each one to the quotation stage where time was lost",
      "Check whether quotes were released after the customer's expected response window",
      "Review missing RFQ inputs, supplier delays, and internal backlog on those cases"
    ],
    nodeRelevanceMap: {
      P008: "Late quote release can directly reduce business take because the customer receives the offer too late to place the order.",
      M001: "Quote OTD is the best direct metric to test whether quotation timing is affecting business take.",
      P001: "Incomplete RFQ intake delays the costing start and shortens the commercial response window.",
      P003: "Supplier quotation delays keep the commercial offer open longer and can delay customer submission.",
      P002: "Insufficient costing capacity slows opportunity handling and can reduce captured business.",
      P006: "Shifting priorities can leave valuable opportunities unfinished or submitted too late.",
      S001: "An RFQ intake gate helps start opportunities with complete data and fewer commercial delays.",
      S004: "Should-cost and supplier response targets reduce wait time on bought parts before submission.",
      S006: "Priority governance protects strategic opportunities from constant interruption."
    }
  },
  {
    id: "flow_timeliness",
    patterns: [/\botd\b/i, /\blead time\b/i, /\bdelay\b/i, /\bbacklog\b/i, /\bcycle time\b/i, /\bdeadline\b/i],
    rationale: "This KPI measures timeliness or flow performance, so delay, backlog, and prioritization nodes are directly relevant.",
    preferredNodeIds: ["M001", "P008", "P002", "P006", "P005", "P003", "S003", "S006"],
    kbHints: [
      "quote OTD",
      "quote released late",
      "insufficient costing capacity",
      "shifting priorities and permanent urgencies",
      "costing rework"
    ],
    ownerFocus: ["Management", "Costing", "Sales"],
    metricFocus: ["Quote OTD", "Average quote lead time", "Backlog days", "WIP count", "Priority changes per week"],
    nextChecks: [
      "Break the delay into intake, costing, supplier, validation, and release stages",
      "Measure backlog and WIP at the same time as the KPI drop",
      "Check how often work sequence changed before completion"
    ],
    nodeRelevanceMap: {
      M001: "This KPI is itself a timing metric or is strongly linked to quote timeliness.",
      P008: "Late release is the visible end effect when the quotation process misses its date.",
      P002: "Capacity shortages create queue buildup and longer lead times.",
      P006: "Frequent reprioritization creates stop-start work and delay accumulation.",
      P005: "Rework adds avoidable cycle time before release.",
      P003: "Supplier waits can extend the total quote timeline.",
      S003: "Flow segmentation helps keep simple and strategic work from being delayed by one shared path.",
      S006: "Priority governance reduces WIP churn and keeps due dates stable."
    }
  },
  {
    id: "cost_margin",
    patterns: [/\bcost\b/i, /\bmargin\b/i, /\bprofit\b/i],
    rationale: "Cost and margin KPIs can be affected by blocked or unstable costing inputs, supplier quotation delays, and rework.",
    preferredNodeIds: ["P003", "P007", "P005", "P004", "S004", "S005", "P002"],
    kbHints: [
      "late supplier quotation",
      "detailed costing blocked",
      "costing rework",
      "should-cost plus supplier SLA",
      "simplify costing model"
    ],
    ownerFocus: ["Purchasing", "Costing", "Management"],
    metricFocus: ["Supplier response lead time", "Time quote stays in draft", "Rework hours per quote", "Manual data entry time", "Throughput per analyst"],
    nextChecks: [
      "Identify which cost lines stayed uncertain longest",
      "Review bought-part quotations and should-cost coverage",
      "Measure how much time was lost to re-entry or recalculation"
    ],
    nodeRelevanceMap: {
      P003: "Late supplier prices can force assumptions or delays in cost completion, affecting margin quality.",
      P007: "Blocked detailed costing leaves key cost lines unresolved and weakens margin confidence.",
      P005: "Rework changes assumptions repeatedly and can destabilize the final cost position.",
      P004: "A heavy costing tool adds manual effort and slows cost-ready decision making.",
      S004: "Should-cost and supplier SLAs provide faster backup pricing for bought parts.",
      S005: "Simplifying the costing model reduces manual effort and repeated assumptions.",
      P002: "Limited analyst capacity can delay detailed cost completion on margin-critical opportunities."
    }
  }
]);

const buildSelectedKpiSummary = (selectedKpi = null) => {
  if (!selectedKpi) return null;

  const currentValue = parseMetricNumber(selectedKpi.current_value);
  const targetValue = parseMetricNumber(selectedKpi.target);
  const lowLimit = parseMetricNumber(selectedKpi.low_limit);
  const highLimit = parseMetricNumber(selectedKpi.high_limit);
  const goodDirection = normalizeKpiDirection(selectedKpi.good_direction) || "up";
  const status = getKpiStatus(currentValue, lowLimit, highLimit, goodDirection);

  const gapToTarget =
    currentValue !== null && targetValue !== null
      ? Number((currentValue - targetValue).toFixed(2))
      : null;

  let targetAssessment = "Target data is not available.";
  if (currentValue !== null && targetValue !== null) {
    if (goodDirection === "down") {
      if (currentValue > targetValue) {
        targetAssessment = `${Number((currentValue - targetValue).toFixed(2))} above target, so improvement is needed.`;
      } else if (currentValue < targetValue) {
        targetAssessment = `${Number((targetValue - currentValue).toFixed(2))} better than target.`;
      } else {
        targetAssessment = "On target.";
      }
    } else if (currentValue < targetValue) {
      targetAssessment = `${Number((targetValue - currentValue).toFixed(2))} below target, so improvement is needed.`;
    } else if (currentValue > targetValue) {
      targetAssessment = `${Number((currentValue - targetValue).toFixed(2))} above target.`;
    } else {
      targetAssessment = "On target.";
    }
  }

  let thresholdAssessment = "Threshold status is not available.";
  if (status.isGood === false) {
    if (goodDirection === "down" && status.upperBound !== null && currentValue !== null) {
      thresholdAssessment = `${Number((currentValue - status.upperBound).toFixed(2))} above the acceptable limit.`;
    } else if (goodDirection === "up" && status.lowerBound !== null && currentValue !== null) {
      thresholdAssessment = `${Number((status.lowerBound - currentValue).toFixed(2))} below the acceptable limit.`;
    } else {
      thresholdAssessment = "Outside the acceptable threshold.";
    }
  } else if (status.isGood === true) {
    thresholdAssessment = "Inside the acceptable threshold.";
  }

  return {
    display_name: buildAssistantKpiDisplayName(selectedKpi.title, selectedKpi.subtitle),
    title: selectedKpi.title || "Untitled KPI",
    subtitle: selectedKpi.subtitle || null,
    unit: selectedKpi.unit || null,
    week: selectedKpi.week || null,
    good_direction: goodDirection,
    current_value: currentValue,
    target: targetValue,
    low_limit: lowLimit,
    high_limit: highLimit,
    gap_to_target: gapToTarget,
    target_assessment: targetAssessment,
    threshold_assessment: thresholdAssessment,
    corrective_action_status: selectedKpi.corrective_action_status || null,
    comment: selectedKpi.comment || null,
    latest_root_cause: selectedKpi.root_cause || null,
    latest_implemented_solution: selectedKpi.implemented_solution || null,
    due_date: selectedKpi.due_date || null,
    responsible: selectedKpi.responsible || null
  };
};

const buildSelectedKpiDelayFocus = (selectedKpi = null) => {
  if (!selectedKpi) {
    return {
      is_direct_match: false,
      matched_rule_id: null,
      rationale: "No KPI is selected, so the knowledge base should be used only if the user explicitly asks about quotation or costing delay.",
      kb_hints: [],
      preferred_node_ids: [],
      node_relevance_map: {},
      owner_focus: [],
      metric_focus: [],
      next_checks: []
    };
  }

  const searchableText = [
    selectedKpi.title,
    selectedKpi.subtitle,
    selectedKpi.comment,
    selectedKpi.root_cause,
    selectedKpi.implemented_solution
  ]
    .filter(Boolean)
    .join(" ");

  const matchedRule = KPI_DELAY_KB_RULES.find((rule) =>
    rule.patterns.some((pattern) => pattern.test(searchableText))
  ) || null;

  if (!matchedRule) {
    return {
      is_direct_match: false,
      matched_rule_id: null,
      rationale: "This KPI does not clearly map to a delay knowledge-base category from its title alone. Use the knowledge base only if the user or KPI context points to quotation, RFQ, costing, supplier, backlog, lead time, or OTD issues.",
      kb_hints: [],
      preferred_node_ids: [],
      node_relevance_map: {},
      owner_focus: [],
      metric_focus: [],
      next_checks: []
    };
  }

  return {
    is_direct_match: true,
    matched_rule_id: matchedRule.id,
    rationale: matchedRule.rationale,
    kb_hints: matchedRule.kbHints,
    preferred_node_ids: matchedRule.preferredNodeIds || [],
    node_relevance_map: matchedRule.nodeRelevanceMap || {},
    owner_focus: matchedRule.ownerFocus || [],
    metric_focus: matchedRule.metricFocus || [],
    next_checks: matchedRule.nextChecks || []
  };
};

const applySelectedKpiKnowledgeBaseFocus = (knowledgeBaseContext, selectedKpiDelayFocus) => {
  if (!knowledgeBaseContext) return knowledgeBaseContext;

  const preferredNodeIds = Array.isArray(selectedKpiDelayFocus?.preferred_node_ids)
    ? selectedKpiDelayFocus.preferred_node_ids
    : [];
  const nodeRelevanceMap = selectedKpiDelayFocus?.node_relevance_map || {};
  const targetMatchCount = Math.max(knowledgeBaseContext.matches?.length || 0, 4);
  const targetRelatedCount = Math.max(knowledgeBaseContext.related?.length || 0, 4);

  const byNodeId = new Map();
  [...(knowledgeBaseContext.matches || []), ...(knowledgeBaseContext.related || [])].forEach((entry) => {
    const key = entry?.node_id || entry?.id;
    if (!key) return;
    const existing = byNodeId.get(key);
    if (!existing || Number(existing.score || 0) < Number(entry.score || 0)) {
      byNodeId.set(key, entry);
    }
  });

  const annotateEntry = (entry) => {
    const preferredIndex = preferredNodeIds.indexOf(entry.node_id);
    const preferredBoost = preferredIndex >= 0
      ? Math.max(0, 220 - (preferredIndex * 14))
      : 0;

    return {
      ...entry,
      kpi_relevance_reason: nodeRelevanceMap[entry.node_id] || null,
      preferred_for_selected_kpi: preferredIndex >= 0,
      kpi_priority_score: Number((Number(entry.score || 0) + preferredBoost).toFixed(2))
    };
  };

  const prioritizedEntries = [...byNodeId.values()]
    .map(annotateEntry)
    .sort((a, b) => Number(b.kpi_priority_score || 0) - Number(a.kpi_priority_score || 0));

  return {
    ...knowledgeBaseContext,
    matches: prioritizedEntries.slice(0, targetMatchCount),
    related: prioritizedEntries.slice(targetMatchCount, targetMatchCount + targetRelatedCount),
    kpi_specific_focus: {
      matched_rule_id: selectedKpiDelayFocus?.matched_rule_id || null,
      preferred_node_ids: preferredNodeIds,
      owner_focus: selectedKpiDelayFocus?.owner_focus || [],
      metric_focus: selectedKpiDelayFocus?.metric_focus || [],
      next_checks: selectedKpiDelayFocus?.next_checks || []
    }
  };
};

const buildKpiScopedKnowledgeBaseQuery = ({
  message,
  selectedKpi,
  selectedKpiSummary,
  selectedKpiDelayFocus
}) => {
  const queryParts = [String(message || "").trim()];

  if (selectedKpiSummary?.display_name) {
    queryParts.push(
      `Selected KPI: ${selectedKpiSummary.display_name}`
    );
  }

  if (selectedKpiSummary?.target_assessment) {
    queryParts.push(`KPI target assessment: ${selectedKpiSummary.target_assessment}`);
  }

  if (selectedKpiSummary?.threshold_assessment) {
    queryParts.push(`KPI threshold assessment: ${selectedKpiSummary.threshold_assessment}`);
  }

  if (selectedKpi?.comment) {
    queryParts.push(`KPI comment: ${selectedKpi.comment}`);
  }

  if (selectedKpiDelayFocus?.kb_hints?.length) {
    queryParts.push(`Delay knowledge-base hints: ${selectedKpiDelayFocus.kb_hints.join(", ")}`);
  }

  if (selectedKpiDelayFocus?.preferred_node_ids?.length) {
    queryParts.push(`Preferred node IDs for this KPI: ${selectedKpiDelayFocus.preferred_node_ids.join(", ")}`);
  }

  return queryParts.filter(Boolean).join("\n");
};

const buildKpiAssistantFallbackReply = ({
  selectedKpiSummary,
  selectedKpiDelayFocus,
  knowledgeBaseContext
}) => {
  const lines = [];

  if (selectedKpiSummary) {
    lines.push(`Focused KPI: ${selectedKpiSummary.display_name}.`);

    if (selectedKpiSummary.current_value !== null || selectedKpiSummary.target !== null) {
      lines.push(
        `Current value: ${selectedKpiSummary.current_value ?? "N/A"}${selectedKpiSummary.unit ? ` ${selectedKpiSummary.unit}` : ""}; target: ${selectedKpiSummary.target ?? "N/A"}${selectedKpiSummary.unit ? ` ${selectedKpiSummary.unit}` : ""}.`
      );
    }

    lines.push(selectedKpiSummary.target_assessment);
    lines.push(selectedKpiSummary.threshold_assessment);
  }

  if (selectedKpiDelayFocus?.rationale) {
    lines.push(`Delay KB relevance: ${selectedKpiDelayFocus.rationale}`);
  }

  if (knowledgeBaseContext?.matches?.length) {
    const topMatches = knowledgeBaseContext.matches.slice(0, 3);
    const kbSummary = topMatches
      .map((entry) => `${entry.node_id} - ${entry.subject}${entry.kpi_relevance_reason ? `: ${entry.kpi_relevance_reason}` : ""}`)
      .join(" | ");
    lines.push(`Most relevant knowledge-base nodes: ${kbSummary}.`);

    const actionSummary = topMatches
      .flatMap((entry) => entry.actions || [])
      .filter(Boolean)
      .slice(0, 4);
    if (actionSummary.length) {
      lines.push(`Recommended actions: ${actionSummary.join("; ")}.`);
    }

    const metricSummary = [
      ...(selectedKpiDelayFocus?.metric_focus || []),
      ...topMatches.flatMap((entry) => entry.metrics || [])
    ].filter(Boolean);
    if (metricSummary.length) {
      lines.push(`Track with: ${[...new Set(metricSummary)].slice(0, 5).join("; ")}.`);
    }
  } else {
    lines.push("No strong delay knowledge-base node matched this KPI and question.");
  }

  if (selectedKpiDelayFocus?.owner_focus?.length) {
    lines.push(`Owner functions to involve: ${selectedKpiDelayFocus.owner_focus.join("; ")}.`);
  }

  if (selectedKpiDelayFocus?.next_checks?.length) {
    lines.push(`Best next checks: ${selectedKpiDelayFocus.next_checks.slice(0, 3).join("; ")}.`);
  }

  return lines.filter(Boolean).join(" ");
};

const isFastKpiAssistantRequest = ({
  message,
  selectedKpi,
  knowledgeBaseContext
}) => {
  if (!selectedKpi) return false;

  const normalizedMessage = String(message || "").trim().toLowerCase();
  if (!normalizedMessage) return true;

  if (normalizedMessage.length <= 80) return true;

  if (
    /\b(analy[sz]e|analysis|why|cause|causes|driver|drivers|action|actions|owner|owners|metric|metrics|what happened|what should|next step|next steps|kb|knowledge base|delay|quotation|quote|rfq|costing)\b/.test(normalizedMessage)
  ) {
    return true;
  }

  return Boolean(knowledgeBaseContext?.diagnostics?.has_strong_match);
};

const buildFastKpiAssistantReply = ({
  selectedKpiSummary,
  selectedKpiDelayFocus,
  knowledgeBaseContext
}) => {
  const lines = [];
  const topMatches = (knowledgeBaseContext?.matches || []).slice(0, 4);
  const topActions = [...new Set(topMatches.flatMap((entry) => entry.actions || []).filter(Boolean))].slice(0, 4);
  const topMetrics = [
    ...(selectedKpiDelayFocus?.metric_focus || []),
    ...topMatches.flatMap((entry) => entry.metrics || [])
  ].filter(Boolean);

  if (selectedKpiSummary) {
    lines.push(`### KPI Status`);
    lines.push(`- **KPI**: ${selectedKpiSummary.display_name}`);

    if (selectedKpiSummary.current_value !== null) {
      lines.push(`- **Current Value**: ${selectedKpiSummary.current_value}${selectedKpiSummary.unit ? ` ${selectedKpiSummary.unit}` : ""}`);
    }

    if (selectedKpiSummary.target !== null) {
      lines.push(`- **Target**: ${selectedKpiSummary.target}${selectedKpiSummary.unit ? ` ${selectedKpiSummary.unit}` : ""}`);
    }

    if (selectedKpiSummary.gap_to_target !== null) {
      const gapDirection = selectedKpiSummary.gap_to_target < 0 ? "below" : "above";
      lines.push(`- **Gap To Target**: ${selectedKpiSummary.gap_to_target}${selectedKpiSummary.unit ? ` ${selectedKpiSummary.unit}` : ""} (${Math.abs(selectedKpiSummary.gap_to_target)} ${gapDirection} target)`);
    }

    lines.push(`- **Target Assessment**: ${selectedKpiSummary.target_assessment}`);
    lines.push(`- **Threshold Assessment**: ${selectedKpiSummary.threshold_assessment}`);
  }

  if (selectedKpiDelayFocus?.rationale) {
    lines.push("");
    lines.push(`The delay knowledge base is relevant here because ${selectedKpiDelayFocus.rationale.charAt(0).toLowerCase()}${selectedKpiDelayFocus.rationale.slice(1)}`);
  }

  if (topMatches.length) {
    lines.push("");
    lines.push(`### Likely Delay-Related Drivers`);
    topMatches.forEach((entry, index) => {
      const reason = entry.kpi_relevance_reason || entry.description || "This node may influence the KPI through the quotation flow.";
      lines.push(`${index + 1}. **${entry.node_id} - ${entry.subject}**: ${reason}`);
    });
  } else {
    lines.push("");
    lines.push(`### Likely Delay-Related Drivers`);
    lines.push(`No strong knowledge-base match was found for this KPI and question.`);
  }

  if (topActions.length) {
    lines.push("");
    lines.push(`### Recommended Actions`);
    topActions.forEach((action, index) => {
      lines.push(`${index + 1}. ${action}`);
    });
  }

  if (selectedKpiDelayFocus?.owner_focus?.length) {
    lines.push("");
    lines.push(`### Owners`);
    selectedKpiDelayFocus.owner_focus.forEach((owner) => {
      lines.push(`- ${owner}`);
    });
  }

  const uniqueMetrics = [...new Set(topMetrics)].slice(0, 5);
  if (uniqueMetrics.length) {
    lines.push("");
    lines.push(`### Metrics To Monitor`);
    uniqueMetrics.forEach((metric) => {
      lines.push(`- ${metric}`);
    });
  }

  if (selectedKpiDelayFocus?.next_checks?.length) {
    lines.push("");
    lines.push(`### Next Checks`);
    selectedKpiDelayFocus.next_checks.slice(0, 3).forEach((check) => {
      lines.push(`- ${check}`);
    });
  }

  return lines.join("\n");
};

const generateKpiAssistantReply = async ({
  responsible,
  week,
  selectedKpiId,
  kpis,
  message
}) => {
  const assistantKpis = buildAssistantKpiContext(kpis);
  const selectedKpi = assistantKpis.find((kpi) =>
    String(kpi.kpi_id) === String(selectedKpiId) ||
    String(kpi.kpi_values_id) === String(selectedKpiId)
  ) || null;
  const selectedKpiSummary = buildSelectedKpiSummary(selectedKpi);
  const selectedKpiDelayFocus = buildSelectedKpiDelayFocus(selectedKpi);
  const knowledgeBaseQuery = buildKpiScopedKnowledgeBaseQuery({
    message,
    selectedKpi,
    selectedKpiSummary,
    selectedKpiDelayFocus
  });
  const rawKnowledgeBaseContext = buildDelayKnowledgeBaseContext(knowledgeBaseQuery, {
    limit: 4,
    relatedLimit: 4,
    preferredNodeIds: selectedKpiDelayFocus.preferred_node_ids || []
  });
  const knowledgeBaseContext = applySelectedKpiKnowledgeBaseFocus(
    rawKnowledgeBaseContext,
    selectedKpiDelayFocus
  );
  const fastLocalReply = buildFastKpiAssistantReply({
    selectedKpiSummary,
    selectedKpiDelayFocus,
    knowledgeBaseContext
  });

  if (isFastKpiAssistantRequest({
    message,
    selectedKpi,
    knowledgeBaseContext
  })) {
    return fastLocalReply;
  }

  const promptKpiContext = selectedKpi ? [selectedKpi] : assistantKpis.slice(0, 8);
  const promptKnowledgeMatches = (knowledgeBaseContext.matches || []).slice(0, 3);
  const promptKnowledgeRelated = (knowledgeBaseContext.related || []).slice(0, 2);

  const prompt = `
You are an AI support assistant for manufacturing teams.

You support two grounded use cases:
1. KPI form interpretation from the page context.
2. Estimating and quotation delay diagnosis from the local knowledge base.

RULES
- Base your answer only on the grounded context below and the user question.
- If a KPI is selected, your answer must be dedicated to that KPI first. Do not give a generic all-KPI answer.
- Start from the selected KPI facts: current value, target, threshold position, and corrective-action status if available.
- Distinguish clearly between confirmed KPI facts and delay-related hypotheses.
- Only connect knowledge-base nodes that could plausibly influence the selected KPI.
- If the selected KPI is not itself a quote-delay KPI, explain the causal path briefly, for example "late quote release can reduce business take".
- Use the KPI-specific relevance notes below when explaining why a node matters for this KPI.
- If the user asks about RFQ, quotation, costing delay, supplier quotes, rework, priorities, backlog, should-cost, owners, or quote OTD, rely on the knowledge base matches first.
- Mention node IDs when you use knowledge base entries, for example P003 or S004.
- Prefer concrete actions, evidence to collect, metrics, owner functions, and linked nodes when helpful.
- If the knowledge base match is weak or incomplete, say that clearly instead of inventing facts.
- If data is missing, say that clearly instead of inventing values.
- Give practical manufacturing-oriented guidance.
- Keep the answer concise and easy to act on.
- Ignore unrelated KPI cards unless the user explicitly asks for comparison.

RESPONSE SHAPE
- If a KPI is selected, structure the answer around that KPI.
- Prefer this flow: KPI status, likely delay-related drivers from the knowledge base if relevant, recommended actions, owners, metrics.
- When listing KB nodes, explain each node's KPI-specific impact in one sentence.

DELAY KNOWLEDGE BASE OVERVIEW
${JSON.stringify(knowledgeBaseContext.overview, null, 2)}

DELAY KNOWLEDGE BASE MATCHES
${JSON.stringify(promptKnowledgeMatches, null, 2)}

DELAY KNOWLEDGE BASE RELATED NODES
${JSON.stringify(promptKnowledgeRelated, null, 2)}

KNOWLEDGE BASE SEARCH DIAGNOSTICS
${JSON.stringify(knowledgeBaseContext.diagnostics, null, 2)}

KNOWLEDGE BASE SEARCH QUERY
${knowledgeBaseQuery}

PAGE CONTEXT
- Responsible: ${responsible?.name || "N/A"}
- Plant: ${responsible?.plant_name || "N/A"}
- Department: ${responsible?.department_name || "N/A"}
- Week: ${week || "N/A"}

SELECTED KPI
${selectedKpi ? JSON.stringify(selectedKpi, null, 2) : "None selected"}

SELECTED KPI SUMMARY
${JSON.stringify(selectedKpiSummary, null, 2)}

SELECTED KPI DELAY-KB FOCUS
${JSON.stringify(selectedKpiDelayFocus, null, 2)}

SELECTED KPI KB PRIORITIES
${JSON.stringify(knowledgeBaseContext.kpi_specific_focus, null, 2)}

KPI CONTEXT FOR PROMPT
${JSON.stringify(promptKpiContext, null, 2)}

USER QUESTION
${message}
`;

  let openai = null;
  try {
    openai = getOpenAIClient();
  } catch (err) {
    return buildKpiAssistantFallbackReply({
      selectedKpiSummary,
      selectedKpiDelayFocus,
      knowledgeBaseContext
    });
  }

  try {
    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        {
          role: "system",
          content: "You are a concise, grounded manufacturing support assistant."
        },
        {
          role: "user",
          content: prompt
        }
      ],
      temperature: 0.2,
      max_tokens: 450
    });

    return (
      completion.choices[0]?.message?.content?.trim() ||
      fastLocalReply ||
      buildKpiAssistantFallbackReply({
        selectedKpiSummary,
        selectedKpiDelayFocus,
        knowledgeBaseContext
      })
    );
  } catch (err) {
    console.error("KPI assistant OpenAI error:", err.message);
    return fastLocalReply || buildKpiAssistantFallbackReply({
      selectedKpiSummary,
      selectedKpiDelayFocus,
      knowledgeBaseContext
    });
  }
};





// ============================================================
// getResponsibleWithKPIs — now also fetches existing corrective
// action data (root_cause, implemented_solution, evidence)
// ============================================================
const getResponsibleWithKPIs = async (responsibleId, week) => {
  const resResp = await pool.query(
    `SELECT r.responsible_id, r.name, r.email, r.plant_id, r.department_id,
             p.name AS plant_name, d.name AS department_name
      FROM public."Responsible" r
      JOIN public."Plant" p ON r.plant_id = p.plant_id
      JOIN public."Department" d ON r.department_id = d.department_id
      WHERE r.responsible_id = $1`,
    [responsibleId]
  );
  const responsible = resResp.rows[0];
  if (!responsible) throw new Error("Responsible not found");

  const kpiRes = await pool.query(
    `SELECT kv.kpi_values_id, kv.value, kv.week, k.kpi_id,
           k.subject, k.indicator_sub_title, k.unit,
           k.target, k.min, k.max, k.tolerance_type,
           k.up_tolerance, k.low_tolerance, k.frequency,
           k.definition, k.calculation_on, k.target_auto_adjustment,
           k.high_limit, k.low_limit,
           COALESCE(
             (SELECT MAX(updated_at) FROM public.kpi_values_hist26
              WHERE kpi_values_id = kv.kpi_values_id), NOW()
           ) as last_updated,
           (SELECT h.comment FROM public.kpi_values_hist26 h
            WHERE h.kpi_values_id = kv.kpi_values_id
              AND h.responsible_id = $1 AND h.week = $2
            ORDER BY h.updated_at DESC LIMIT 1) as latest_comment
    FROM public.kpi_values kv
    JOIN "Kpi" k ON kv.kpi_id = k.kpi_id
    WHERE kv.responsible_id = $1 AND kv.week = $2
    ORDER BY k.kpi_id ASC`,
    [responsibleId, week]
  );

  const correctiveActionsRes = await pool.query(
    `SELECT corrective_action_id, responsible_id, kpi_id, week,
            root_cause, implemented_solution, evidence,
            status, created_date, updated_date, due_date, responsible
     FROM public.corrective_actions
     WHERE responsible_id = $1 AND week = $2
     ORDER BY kpi_id ASC, COALESCE(updated_date, created_date) ASC, corrective_action_id ASC`,
    [responsibleId, week]
  );

  const actionsByKpiId = {};
  correctiveActionsRes.rows.forEach((action) => {
    if (!actionsByKpiId[action.kpi_id]) {
      actionsByKpiId[action.kpi_id] = [];
    }
    actionsByKpiId[action.kpi_id].push(action);
  });

  const kpis = kpiRes.rows.map((kpi) => {
    const correctiveActions = sortCorrectiveActions(actionsByKpiId[kpi.kpi_id] || []);
    const latestCorrectiveAction = getLatestCorrectiveAction(correctiveActions);

    return {
      ...kpi,
      corrective_actions: correctiveActions,
      corrective_action_count: correctiveActions.length,
      corrective_action_id: latestCorrectiveAction?.corrective_action_id ?? null,
      ca_root_cause: latestCorrectiveAction?.root_cause ?? "",
      ca_implemented_solution: latestCorrectiveAction?.implemented_solution ?? "",
      ca_evidence: latestCorrectiveAction?.evidence ?? "",
      ca_status: latestCorrectiveAction?.status ?? "",
      ca_due_date: latestCorrectiveAction?.due_date ?? null,
      ca_responsible_name: latestCorrectiveAction?.responsible ?? ""
    };
  });

  return { responsible, kpis };
};

const generateEmailHtml = ({ responsible, week }) => {
  // Convert week format (e.g., "2026-Week12") to month and year
  const getMonthYearFromWeek = (weekStr) => {
    const match = weekStr.match(/(\d{4})-Week(\d+)/);
    if (!match) return weekStr; // Return original if format doesn't match

    const year = parseInt(match[1]);
    const weekNumber = parseInt(match[2]);

    // Calculate approximate date from week number (week 1 starts around Jan 1)
    const date = new Date(year, 0, 1 + (weekNumber - 1) * 7);

    // Return month and year (e.g., "March 2026")
    return date.toLocaleString('en-US', { month: 'long', year: 'numeric' });
  };

  const monthYear = getMonthYearFromWeek(week);

  return `
  <!DOCTYPE html><html><head><meta charset="utf-8"><title>KPI Form</title></head>
  <body style="font-family:'Segoe UI',sans-serif;background:#f4f4f4;padding:20px;margin:0;">
    <div style="max-width:600px;margin:0 auto;background:#fff;padding:30px 25px 40px;border-radius:10px;box-shadow:0 4px 15px rgba(0,0,0,0.1);text-align:center;">
      
      <!-- Logo -->
      <div style="margin-bottom:10px;">
        <img src="https://media.licdn.com/dms/image/v2/D4E0BAQGYVmAPO2RZqQ/company-logo_200_200/company-logo_200_200/0/1689240189455/avocarbon_group_logo?e=2147483647&v=beta&t=nZNCXd3ypoMFQnQMxfAZrljyNBbp4E5HM11Y1yl9_L0"
             alt="AVOCarbon Logo" style="width:90px;height:90px;object-fit:contain;">
      </div>
      
      <!-- KPI codir with cadre/border - directly under logo and centered -->
      <div style="border:2px solid #0078D7;border-radius:8px;padding:6px 25px;display:inline-block;margin:0 auto 15px auto;">
        <span style="color:#0078D7;font-size:16px;font-weight:500;display:inline-block;">KPI codir</span>
      </div>
      
      <h2 style="color:#0078D7;font-size:24px;margin:0 0 10px 0;font-weight:600;">KPI Submission - ${monthYear}</h2>
      
      <h3 style="color:#333;font-size:18px;margin:0 0 25px 0;font-weight:500;">${responsible.plant_name}</h3>
      
      <a href="https://kpi-codir.azurewebsites.net/form?responsible_id=${responsible.responsible_id}&week=${week}"
         style="display:inline-block;padding:14px 35px;background:#0078D7;color:white;
                border-radius:50px;text-decoration:none;font-weight:600;font-size:16px;
                margin-bottom:20px;border:none;cursor:pointer;">
        Fill KPI Form
      </a>
      
      <p style="margin:0;font-size:13px;color:#666;">Click the button above to fill your KPIs for ${monthYear}.</p>
    </div>
  </body></html>`;
};


const checkAndTriggerCorrectiveActions = async (responsibleId, kpiId, week, newValue, histId) => {
  try {
    const kpiRes = await pool.query(
      `SELECT target FROM public."Kpi" WHERE kpi_id = $1`, [kpiId]
    );
    if (!kpiRes.rows.length) return { targetUpdated: false };

    const currentTarget = parseFloat(kpiRes.rows[0].target);
    const numValue = parseFloat(newValue);
    if (isNaN(numValue) || isNaN(currentTarget)) return { targetUpdated: false };

    // ── value exceeds current target → queue it, touch NOTHING else ──────────
    if (numValue > currentTarget) {
      console.log(`📌 KPI ${kpiId}: ${numValue} > target ${currentTarget} — queuing pending update`);

      await pool.query(
        `INSERT INTO public.pending_target_updates
           (kpi_id, responsible_id, week, new_target, applied)
         VALUES ($1, $2, $3, $4, false)
         ON CONFLICT (kpi_id, responsible_id, week)
         DO UPDATE SET new_target = EXCLUDED.new_target, applied = false`,
        [kpiId, responsibleId, week, String(numValue)]
      );

      console.log(`✅ Pending target queued — KPI ${kpiId}: ${currentTarget} → ${numValue}`);

      return {
        targetUpdated: true,
        updateInfo: { kpiId, oldTarget: currentTarget, newTarget: numValue }
      };
    }

    return { targetUpdated: false };

  } catch (error) {
    console.error('❌ checkAndTriggerCorrectiveActions error:', error.message);
    return { targetUpdated: false, error: error.message };
  }
};

// ============================================================
// upsertCorrectiveAction — saves root_cause, implemented_solution,
// evidence into corrective_actions table for a given kpi/week
// ============================================================
const upsertCorrectiveAction = async (
  responsibleId,
  kpiId,
  week,
  { correctiveActionId, rootCause, implementedSolution, dueDate, responsibleName, status }
) => {
  try {
    const normalizedPayload = {
      rootCause: normalizeText(rootCause),
      implementedSolution: normalizeText(implementedSolution),
      dueDate: normalizeText(dueDate),
      responsibleName: normalizeText(responsibleName),
      status: "Open"
    };
    let resolvedStatus = getCorrectiveActionStatusValue(normalizedPayload);

    if (normalizeText(correctiveActionId) && !normalizedPayload.status) {
      const existingStatusRes = await pool.query(
        `SELECT status
         FROM public.corrective_actions
         WHERE corrective_action_id = $1
         LIMIT 1`,
        [correctiveActionId]
      );
      resolvedStatus = normalizeText(existingStatusRes.rows[0]?.status) || "Open";
    }

    if (normalizeText(correctiveActionId)) {
      const result = await pool.query(
        `UPDATE public.corrective_actions
     SET root_cause = $2::text,
       implemented_solution = $3::text,
       due_date = $4::date,
       responsible = $5::text,
       status = $6::text,
       updated_date = NOW()
      WHERE corrective_action_id = $1
       RETURNING corrective_action_id`,
        [
          correctiveActionId,
          normalizedPayload.rootCause,
          normalizedPayload.implementedSolution,
          normalizedPayload.dueDate,
          normalizedPayload.responsibleName,
          normalizedPayload.status
        ]
      );

      if (result.rowCount > 0) {
        return true;
      }
    }

    await pool.query(
      `INSERT INTO public.corrective_actions
   (
     responsible_id, kpi_id, week,
     root_cause, implemented_solution,
     due_date, responsible, status
   )
   VALUES (
     $1,$2,$3,$4::text,$5::text,$6::date,$7::text,$8::text
   )`,
      [
        responsibleId,
        kpiId,
        week,
        normalizedPayload.rootCause,
        normalizedPayload.implementedSolution,
        normalizedPayload.dueDate,
        normalizedPayload.responsibleName,
        normalizedPayload.status
      ]
    );

    return true;
  } catch (err) {
    console.error("upsertCorrectiveAction error:", err.message);
    return false;
  }
};
// ============================================================
// AI SUGGESTION HELPER — generates 2 CA suggestions for a KPI
// ============================================================
const generateCASuggestions = async (kpi) => {
  try {
    const openai = getOpenAIClient();
    const goodDirection = inferKpiDirection(kpi);

    const currentVal = parseFloat(kpi.value || 0);
    const targetVal = parseFloat(kpi.target || 0);
    const lowLimit = parseFloat(kpi.low_limit || 0);
    const highLimit = parseFloat(kpi.high_limit || 0);

    const gapToTarget = !isNaN(targetVal) ? (targetVal - currentVal).toFixed(2) : "N/A";
    const gapToLow = !isNaN(lowLimit) ? (lowLimit - currentVal).toFixed(2) : "N/A";

    const prompt = `
You are a senior industrial performance manager in manufacturing.

Your task is to write 2 highly practical corrective action suggestions for a KPI that is underperforming.

KPI CONTEXT
- KPI: ${kpi.subject || "N/A"}
- Subtitle: ${kpi.indicator_sub_title || "N/A"}
- Definition: ${kpi.definition || "N/A"}
- Calculation basis: ${kpi.calculation_on || "N/A"}
- Frequency: ${kpi.frequency || "N/A"}
- Unit: ${kpi.unit || "N/A"}
- Current value: ${currentVal}
- Target: ${targetVal || "N/A"}
- Low limit: ${kpi.low_limit || "N/A"}
- High limit: ${kpi.high_limit || "N/A"}
- Good direction: ${goodDirection === "down" ? "Down (lower is better)" : "Up (higher is better)"}
- Gap to target: ${gapToTarget}
- Gap to low limit: ${gapToLow}

INSTRUCTIONS
- Give exactly 2 different hypotheses.
- Be specific to the KPI context.
- Use direct operational language.
- Avoid generic phrases like "improve monitoring" or "optimize process" unless you say exactly how.
- The immediate action must be executable by a plant/department responsible.
- Evidence must be measurable and concrete.
- Mention exact checks, exact actions, and exact proof.
- Keep each field short but precise.
- Write like an industrial manager, not like a consultant.

Return ONLY valid JSON with this format:
{
  "suggestion_1": {
    "root_cause": "...",
    "immediate_action": "...",
    "evidence": "..."
  },
  "suggestion_2": {
    "root_cause": "...",
    "immediate_action": "...",
    "evidence": "..."
  }
}
`;

    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [{ role: "user", content: prompt }],
      temperature: 0.35,
      max_tokens: 700,
    });

    const raw = completion.choices[0].message.content.trim().replace(/```json|```/g, "").trim();
    const parsed = JSON.parse(raw);

    return [
      {
        root_cause: parsed.suggestion_1?.root_cause || "",
        immediate_action: parsed.suggestion_1?.immediate_action || "",
        evidence: parsed.suggestion_1?.evidence || "",
      },
      {
        root_cause: parsed.suggestion_2?.root_cause || "",
        immediate_action: parsed.suggestion_2?.immediate_action || "",
        evidence: parsed.suggestion_2?.evidence || "",
      },
    ];
  } catch (err) {
    console.error("AI suggestion error:", kpi.subject, err.message);
    return null;
  }
};

// GET /generate-ca-suggestion?kpi_id=&responsible_id=&week=
app.get("/generate-ca-suggestion", async (req, res) => {
  try {
    const { kpi_id, responsible_id, week } = req.query;

    const kpiRes = await pool.query(
      `SELECT k.kpi_id, k.subject, k.indicator_sub_title,
              k.unit, k.target, k.definition, k.calculation_on,
              k.frequency, k.low_limit, k.high_limit, kv.value
       FROM public."Kpi" k
       LEFT JOIN public.kpi_values kv
         ON kv.kpi_id = k.kpi_id
        AND kv.responsible_id = $2
        AND kv.week = $3
       WHERE k.kpi_id = $1
       LIMIT 1`,
      [kpi_id, responsible_id, week]
    );

    if (!kpiRes.rows.length)
      return res.status(404).json({ error: "KPI not found" });

    const suggestions = await generateCASuggestions(kpiRes.rows[0]);

    if (!suggestions || suggestions.length === 0)
      return res.status(500).json({ error: "Could not generate suggestion" });

    // Randomly pick one suggestion to give variety on re-generate
    const suggestion = suggestions[Math.floor(Math.random() * suggestions.length)];
    res.json({ suggestion });
  } catch (err) {
    console.error("Error generating CA suggestion:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post("/kpi-ai-assistant", async (req, res) => {
  try {
    const {
      responsible_id,
      week,
      selected_kpi_id,
      message,
      kpis
    } = req.body || {};

    const cleanMessage = String(message || "").trim();
    if (!cleanMessage) {
      return res.status(400).json({ error: "Message is required" });
    }

    let responsible = null;
    if (responsible_id) {
      const responsibleRes = await pool.query(
        `SELECT r.responsible_id, r.name, r.email, r.plant_id, r.department_id,
                p.name AS plant_name, d.name AS department_name
         FROM public."Responsible" r
         JOIN public."Plant" p ON r.plant_id = p.plant_id
         JOIN public."Department" d ON r.department_id = d.department_id
         WHERE r.responsible_id = $1`,
        [responsible_id]
      );
      responsible = responsibleRes.rows[0] || null;
    }

    const reply = await generateKpiAssistantReply({
      responsible,
      week,
      selectedKpiId: selected_kpi_id,
      kpis: Array.isArray(kpis) ? kpis : [],
      message: cleanMessage
    });

    res.json({ reply });
  } catch (err) {
    console.error("KPI AI assistant error:", err.message);
    res.status(500).json({ error: err.message || "Could not generate AI response" });
  }
});

// ========== CORRECTIVE ACTION ROUTES ==========
app.get("/corrective-actions-list", async (req, res) => {
  try {
    const { responsible_id } = req.query;

    if (!responsible_id) {
      return res.status(400).send(`<p style="color:red;">Missing responsible_id</p>`);
    }

    const responsibleRes = await pool.query(
      `SELECT r.responsible_id, r.name, r.email,
              p.name AS plant_name,
              d.name AS department_name
       FROM public."Responsible" r
       JOIN public."Plant" p ON r.plant_id = p.plant_id
       JOIN public."Department" d ON r.department_id = d.department_id
       WHERE r.responsible_id = $1`,
      [responsible_id]
    );

    const responsible = responsibleRes.rows[0];
    if (!responsible) {
      return res.status(404).send(`<p style="color:red;">Responsible not found</p>`);
    }

    const actionsRes = await pool.query(
      `SELECT
         ca.*,
         ROW_NUMBER() OVER (
           PARTITION BY ca.kpi_id, ca.week
           ORDER BY COALESCE(ca.updated_date, ca.created_date) ASC, ca.corrective_action_id ASC
         ) AS action_number,
         k.subject AS indicator_title,
         k.indicator_sub_title,
         k.unit,
         kv.value,
         k.target,
         k.low_limit,
         k.high_limit
       FROM public.corrective_actions ca
       JOIN public."Kpi" k
         ON ca.kpi_id = k.kpi_id
       LEFT JOIN public.kpi_values kv
         ON kv.kpi_id = ca.kpi_id
        AND kv.responsible_id = ca.responsible_id
        AND kv.week = ca.week
       WHERE ca.responsible_id = $1
       ORDER BY
         CASE
           WHEN ca.due_date IS NULL THEN 1
           WHEN ca.due_date < CURRENT_DATE
                AND ca.status NOT IN ('Closed', 'Completed') THEN 0
           ELSE 1
         END,
         ca.due_date ASC NULLS LAST,
         ca.created_date DESC`,
      [responsible_id]
    );

    const actions = actionsRes.rows;

    const formatDate = (dateValue) => {
      if (!dateValue) return "—";
      const d = new Date(dateValue);
      if (isNaN(d.getTime())) return "—";
      return d.toLocaleDateString("en-GB", {
        day: "2-digit",
        month: "short",
        year: "numeric"
      });
    };

    const escapeHtml = (value) =>
      String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");

    const cardsHtml = actions.length === 0
      ? `
        <div class="empty-state">
          <div class="empty-icon">📭</div>
          <h3>No corrective actions found</h3>
          <p>This responsible does not have corrective actions yet.</p>
        </div>
      `
      : actions.map((a) => {
        const today = new Date();
        today.setHours(0, 0, 0, 0);

        const dueDate = a.due_date ? new Date(a.due_date) : null;
        const isOverdue =
          dueDate &&
          !isNaN(dueDate.getTime()) &&
          dueDate < today &&
          !["Closed", "Completed"].includes(a.status);

        const currentVal = a.value !== null && a.value !== undefined && a.value !== ""
          ? parseFloat(a.value)
          : null;

        const targetVal = a.target !== null && a.target !== undefined && a.target !== ""
          ? parseFloat(a.target)
          : null;

        const gap =
          currentVal !== null && !isNaN(currentVal) &&
            targetVal !== null && !isNaN(targetVal)
            ? (targetVal - currentVal).toFixed(2)
            : "—";

        const safeStatusClass = String(a.status || "open")
          .toLowerCase()
          .replace(/\s+/g, "-");

        return `
            <div class="ca-card ${isOverdue ? "overdue" : ""}">
              <div class="ca-card-top">
                <div>
                  <div class="ca-kpi-title">${escapeHtml(a.indicator_title)}</div>
                  <div class="ca-kpi-subtitle">Action ${escapeHtml(a.action_number || 1)}</div>
                  ${a.indicator_sub_title
            ? `<div class="ca-kpi-subtitle">${escapeHtml(a.indicator_sub_title)}</div>`
            : ""}
                </div>
                <span class="status-pill status-${safeStatusClass}">
                  ${escapeHtml(a.status || "Open")}
                </span>
              </div>

              <div class="ca-stats">
                <div class="stat-box">
                  <span class="stat-label">Current</span>
                  <strong>${a.value ?? "—"} ${escapeHtml(a.unit || "")}</strong>
                </div>
                <div class="stat-box">
                  <span class="stat-label">Target</span>
                  <strong>${a.target ?? "—"} ${escapeHtml(a.unit || "")}</strong>
                </div>
                <div class="stat-box">
                  <span class="stat-label">Gap</span>
                  <strong>${gap} ${escapeHtml(a.unit || "")}</strong>
                </div>
              </div>

              <div class="ca-meta-grid">
                <div class="meta-item">
                  <span>Week</span>
                  <strong>${escapeHtml(a.week)}</strong>
                </div>
                <div class="meta-item">
                  <span>Responsible</span>
                  <strong>${escapeHtml(a.responsible || responsible.name || "—")}</strong>
                </div>
                <div class="meta-item ${isOverdue ? "deadline-overdue" : ""}">
                  <span>Due Date</span>
                  <strong>${formatDate(a.due_date)}</strong>
                </div>
                <div class="meta-item">
                  <span>Created</span>
                  <strong>${formatDate(a.created_date)}</strong>
                </div>
                <div class="meta-item">
                  <span>Updated</span>
                  <strong>${formatDate(a.updated_date)}</strong>
                </div>
              </div>

              <div class="ca-footer">
                <div class="deadline-badge ${isOverdue ? "deadline-badge-overdue" : ""}">
                  ${a.due_date
            ? (isOverdue ? "⛔ Overdue" : "📅 Due date set")
            : "🕒 No due date"}
                </div>

                <a class="view-btn"
                   href="/corrective-action-form?responsible_id=${encodeURIComponent(responsible_id)}&kpi_id=${encodeURIComponent(a.kpi_id)}&week=${encodeURIComponent(a.week)}&corrective_action_id=${encodeURIComponent(a.corrective_action_id)}">
                  ${a.status === "Open" ? "Complete Action" : "View Corrective Action"}
                </a>
              </div>
            </div>
          `;
      }).join("");

    res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1.0">
        <title>Corrective Actions</title>
        <style>
          *{box-sizing:border-box}
          body{
            margin:0;
            padding:24px;
            font-family:'Segoe UI',system-ui,sans-serif;
            background:linear-gradient(135deg,#eef2ff 0%,#f8fafc 100%);
            color:#1f2937;
          }
          .container{
            max-width:1200px;
            margin:0 auto;
          }
          .header{
            background:linear-gradient(135deg,#0f172a,#1d4ed8);
            color:white;
            border-radius:18px;
            padding:28px;
            box-shadow:0 10px 30px rgba(0,0,0,0.12);
            margin-bottom:24px;
          }
          .header h1{
            margin:0 0 10px;
            font-size:30px;
          }
          .header-sub{
            display:flex;
            flex-wrap:wrap;
            gap:10px 18px;
            opacity:.95;
            font-size:14px;
          }
          .cards-grid{
            display:grid;
            grid-template-columns:repeat(auto-fit,minmax(340px,1fr));
            gap:20px;
          }
          .ca-card{
            background:white;
            border-radius:18px;
            padding:22px;
            border:1px solid #e5e7eb;
            box-shadow:0 8px 24px rgba(15,23,42,0.06);
            transition:transform .2s ease, box-shadow .2s ease;
          }
          .ca-card:hover{
            transform:translateY(-3px);
            box-shadow:0 14px 34px rgba(15,23,42,0.10);
          }
          .ca-card.overdue{
            border:1px solid #ef4444;
            box-shadow:0 10px 28px rgba(239,68,68,0.12);
          }
          .ca-card-top{
            display:flex;
            justify-content:space-between;
            gap:14px;
            align-items:flex-start;
            margin-bottom:16px;
          }
          .ca-kpi-title{
            font-size:18px;
            font-weight:800;
            color:#111827;
            line-height:1.3;
          }
          .ca-kpi-subtitle{
            margin-top:6px;
            font-size:13px;
            color:#6b7280;
          }
          .status-pill{
            display:inline-flex;
            align-items:center;
            padding:8px 12px;
            border-radius:999px;
            font-size:12px;
            font-weight:700;
            white-space:nowrap;
            border:1px solid transparent;
          }
          .status-open{
            background:#fef2f2;
            color:#b91c1c;
            border-color:#fecaca;
          }
          .status-waiting-for-validation{
            background:#fff7ed;
            color:#c2410c;
            border-color:#fed7aa;
          }
          .status-completed,.status-closed{
            background:#ecfdf5;
            color:#047857;
            border-color:#a7f3d0;
          }
          .ca-stats{
            display:grid;
            grid-template-columns:repeat(3,1fr);
            gap:12px;
            margin-bottom:16px;
          }
          .stat-box{
            background:#f8fafc;
            border:1px solid #e5e7eb;
            border-radius:12px;
            padding:12px;
            text-align:center;
          }
          .stat-label{
            display:block;
            font-size:11px;
            text-transform:uppercase;
            letter-spacing:.6px;
            color:#64748b;
            margin-bottom:6px;
            font-weight:700;
          }
          .stat-box strong{
            font-size:16px;
            color:#111827;
          }
          .ca-meta-grid{
            display:grid;
            grid-template-columns:repeat(2,1fr);
            gap:12px;
            margin-bottom:18px;
          }
          .meta-item{
            background:#f9fafb;
            border:1px solid #e5e7eb;
            border-radius:12px;
            padding:12px;
          }
          .meta-item span{
            display:block;
            font-size:11px;
            text-transform:uppercase;
            letter-spacing:.5px;
            color:#6b7280;
            margin-bottom:5px;
            font-weight:700;
          }
          .meta-item strong{
            font-size:14px;
            color:#111827;
          }
          .deadline-overdue{
            background:#fef2f2;
            border-color:#fecaca;
          }
          .deadline-overdue strong{
            color:#b91c1c;
          }
          .ca-footer{
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:12px;
            flex-wrap:wrap;
          }
          .deadline-badge{
            display:inline-flex;
            align-items:center;
            padding:8px 12px;
            border-radius:999px;
            background:#eff6ff;
            color:#1d4ed8;
            font-size:12px;
            font-weight:700;
          }
          .deadline-badge-overdue{
            background:#fef2f2;
            color:#b91c1c;
          }
          .view-btn{
            display:inline-block;
            text-decoration:none;
            background:linear-gradient(135deg,#2563eb,#1d4ed8);
            color:white;
            font-weight:700;
            padding:11px 16px;
            border-radius:12px;
            box-shadow:0 8px 20px rgba(37,99,235,0.22);
          }
          .view-btn:hover{
            opacity:.95;
          }
          .empty-state{
            background:white;
            border-radius:18px;
            padding:60px 24px;
            text-align:center;
            border:1px dashed #cbd5e1;
          }
          .empty-icon{
            font-size:48px;
            margin-bottom:10px;
          }
          .back-row{
            margin-top:22px;
            text-align:center;
          }
          .back-link{
            display:inline-block;
            text-decoration:none;
            color:#1d4ed8;
            font-weight:700;
          }
          @media(max-width:700px){
            body{padding:16px}
            .ca-stats,.ca-meta-grid{grid-template-columns:1fr}
            .ca-card-top,.ca-footer{flex-direction:column;align-items:flex-start}
          }
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h1>📋 Corrective Actions</h1>
            <div class="header-sub">
              <div><strong>Responsible:</strong> ${escapeHtml(responsible.name)}</div>
              <div><strong>Plant:</strong> ${escapeHtml(responsible.plant_name)}</div>
              <div><strong>Department:</strong> ${escapeHtml(responsible.department_name)}</div>
            </div>
          </div>

          <div class="cards-grid">
            ${cardsHtml}
          </div>

          <div class="back-row">
            <a class="back-link" href="/dashboard?responsible_id=${encodeURIComponent(responsible_id)}">
              ← Back to Dashboard
            </a>
          </div>
        </div>
      </body>
      </html>
    `);
  } catch (err) {
    console.error("Error loading corrective actions list:", err);
    res.status(500).send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});

app.get("/corrective-action-form", async (req, res) => {
  try {
    const { responsible_id, kpi_id, week, corrective_action_id, new_action } = req.query;

    if (!responsible_id || !kpi_id || !week) {
      return res.status(400).send("Missing responsible_id, kpi_id, or week");
    }

    const resResp = await pool.query(
      `SELECT r.*, p.name AS plant_name, d.name AS department_name
       FROM public."Responsible" r
       JOIN public."Plant" p ON r.plant_id = p.plant_id
       JOIN public."Department" d ON r.department_id = d.department_id
       WHERE r.responsible_id = $1`,
      [responsible_id]
    );

    const responsible = resResp.rows[0];
    if (!responsible) {
      return res.status(404).send("Responsible not found");
    }

    const kpiResp = await pool.query(
      `SELECT
         k.kpi_id,
         k.subject AS indicator_title,
         k.indicator_sub_title,
         k.unit,
         k.target,
         k.low_limit,
         k.high_limit,
         kv.value
       FROM public."Kpi" k
       LEFT JOIN public.kpi_values kv
         ON k.kpi_id = kv.kpi_id
        AND kv.responsible_id = $2
        AND kv.week = $3
       WHERE k.kpi_id = $1
       LIMIT 1`,
      [kpi_id, responsible_id, week]
    );

    const kpi = kpiResp.rows[0];
    if (!kpi) {
      return res.status(404).send("KPI not found");
    }

    let ed = {};
    if (corrective_action_id) {
      const existingCARes = await pool.query(
        `SELECT *
         FROM public.corrective_actions
         WHERE corrective_action_id = $1
           AND responsible_id = $2
           AND kpi_id = $3
           AND week = $4
         LIMIT 1`,
        [corrective_action_id, responsible_id, kpi_id, week]
      );
      ed = existingCARes.rows[0] || {};
    } else if (String(new_action || "") !== "1") {
      const existingCARes = await pool.query(
        `SELECT *
         FROM public.corrective_actions
         WHERE responsible_id = $1
           AND kpi_id = $2
           AND week = $3
         ORDER BY COALESCE(updated_date, created_date) DESC, corrective_action_id DESC
         LIMIT 1`,
        [responsible_id, kpi_id, week]
      );
      ed = existingCARes.rows[0] || {};
    }

    const escapeHtml = (value) =>
      String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");

    const defaultDueDate = (() => {
      if (ed.due_date) return formatInputDate(ed.due_date);
      const d = new Date();
      d.setDate(d.getDate() + 7);
      return d.toISOString().split("T")[0];
    })();

    const defaultResponsibleName = escapeHtml(ed.responsible || responsible.name || "");
    const actionTitle = ed.corrective_action_id
      ? `Corrective Action #${escapeHtml(ed.corrective_action_id)}`
      : "New Corrective Action";
    const submitLabel = ed.corrective_action_id
      ? "Update Corrective Action"
      : "Create Corrective Action";

    res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1.0">
        <title>Corrective Action</title>
        <style>
          *{box-sizing:border-box}
          body{
            margin:0;
            padding:24px;
            font-family:'Segoe UI',system-ui,sans-serif;
            background:linear-gradient(135deg,#eef2ff 0%,#f8fafc 100%);
            color:#1f2937;
          }
          .container{
            max-width:900px;
            margin:0 auto;
            background:white;
            border-radius:22px;
            overflow:hidden;
            box-shadow:0 20px 50px rgba(15,23,42,0.10);
            border:1px solid #e5e7eb;
          }
          .header{
            background:linear-gradient(135deg,#b91c1c,#ef4444);
            color:white;
            padding:30px;
          }
          .header h1{
            margin:0 0 10px;
            font-size:30px;
          }
          .header .sub{
            display:flex;
            flex-wrap:wrap;
            gap:10px 18px;
            font-size:14px;
            opacity:.96;
          }
          .body{
            padding:30px;
          }
          .info-grid{
            display:grid;
            grid-template-columns:repeat(2,1fr);
            gap:14px;
            margin-bottom:24px;
          }
          .info-box{
            background:#f8fafc;
            border:1px solid #e5e7eb;
            border-radius:14px;
            padding:14px 16px;
          }
          .info-box span{
            display:block;
            font-size:11px;
            text-transform:uppercase;
            letter-spacing:.6px;
            color:#64748b;
            font-weight:700;
            margin-bottom:6px;
          }
          .info-box strong{
            color:#111827;
            font-size:15px;
          }
          .section-title{
            margin:0 0 18px;
            font-size:20px;
            color:#b91c1c;
          }
          .form-group{
            margin-bottom:18px;
          }
          label{
            display:block;
            font-weight:700;
            margin-bottom:8px;
            color:#374151;
            font-size:14px;
          }
          textarea,input[type="date"],input[type="text"]{
            width:100%;
            padding:14px 15px;
            border:1px solid #d1d5db;
            border-radius:12px;
            font-size:14px;
            font-family:inherit;
            background:#fff;
            transition:border-color .2s, box-shadow .2s;
          }
          textarea{
            min-height:120px;
            resize:vertical;
          }
          textarea:focus,input[type="date"]:focus,input[type="text"]:focus{
            outline:none;
            border-color:#2563eb;
            box-shadow:0 0 0 4px rgba(37,99,235,0.10);
          }
          .dates-grid{
            display:grid;
            grid-template-columns:repeat(2,1fr);
            gap:14px;
          }
          .status-line{
            margin-bottom:18px;
          }
          .status-badge{
            display:inline-flex;
            align-items:center;
            padding:8px 12px;
            border-radius:999px;
            font-size:12px;
            font-weight:800;
            border:1px solid transparent;
          }
          .status-open{
            background:#fef2f2;
            color:#b91c1c;
            border-color:#fecaca;
          }
          .status-waiting-for-validation{
            background:#fff7ed;
            color:#c2410c;
            border-color:#fed7aa;
          }
          .status-completed,.status-closed{
            background:#ecfdf5;
            color:#047857;
            border-color:#a7f3d0;
          }
          .actions{
            display:flex;
            gap:12px;
            margin-top:26px;
            flex-wrap:wrap;
          }
          .submit-btn{
            background:linear-gradient(135deg,#b91c1c,#ef4444);
            color:white;
            border:none;
            padding:14px 28px;
            border-radius:12px;
            font-size:15px;
            font-weight:800;
            cursor:pointer;
            box-shadow:0 10px 24px rgba(239,68,68,0.22);
          }
          .back-btn{
            display:inline-block;
            padding:14px 22px;
            border-radius:12px;
            text-decoration:none;
            font-weight:800;
            color:#1d4ed8;
            background:#eff6ff;
          }
          .hint{
            margin-top:6px;
            font-size:12px;
            color:#6b7280;
          }
          @media(max-width:760px){
            body{padding:16px}
            .info-grid,.dates-grid{grid-template-columns:1fr}
            .body{padding:20px}
          }
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h1>${actionTitle}</h1>
            <div class="sub">
              <div><strong>Week:</strong> ${escapeHtml(week)}</div>
              <div><strong>Responsible:</strong> ${escapeHtml(responsible.name)}</div>
              <div><strong>Plant:</strong> ${escapeHtml(responsible.plant_name)}</div>
              <div><strong>Department:</strong> ${escapeHtml(responsible.department_name)}</div>
            </div>
          </div>

          <div class="body">
            <div class="info-grid">
              <div class="info-box">
                <span>KPI</span>
                <strong>${escapeHtml(kpi.indicator_title)}</strong>
              </div>
              <div class="info-box">
                <span>Subtitle</span>
                <strong>${escapeHtml(kpi.indicator_sub_title || "—")}</strong>
              </div>
              <div class="info-box">
                <span>Current Value</span>
                <strong>${kpi.value ?? "—"} ${escapeHtml(kpi.unit || "")}</strong>
              </div>
              <div class="info-box">
                <span>Target</span>
                <strong>${kpi.target ?? "—"} ${escapeHtml(kpi.unit || "")}</strong>
              </div>
              <div class="info-box">
                <span>Low Limit</span>
                <strong>${kpi.low_limit ?? "—"} ${escapeHtml(kpi.unit || "")}</strong>
              </div>
              <div class="info-box">
                <span>High Limit</span>
                <strong>${kpi.high_limit ?? "—"} ${escapeHtml(kpi.unit || "")}</strong>
              </div>
            </div>

            <div class="status-line">
              <span class="status-badge ${String(ed.status || "Open").toLowerCase().replace(/\s+/g, "-").includes("waiting-for-validation") ? "status-waiting-for-validation" : String(ed.status || "Open").toLowerCase().replace(/\s+/g, "-").includes("completed") || String(ed.status || "Open").toLowerCase().replace(/\s+/g, "-").includes("closed") ? "status-completed" : "status-open"}">
                Status: ${escapeHtml(ed.status || "Open")}
              </span>
            </div>

            <h2 class="section-title">Action Details</h2>

            <form action="/submit-corrective-action" method="POST">
              <input type="hidden" name="responsible_id" value="${escapeHtml(responsible_id)}">
              <input type="hidden" name="kpi_id" value="${escapeHtml(kpi_id)}">
              <input type="hidden" name="week" value="${escapeHtml(week)}">
              ${ed.corrective_action_id
        ? `<input type="hidden" name="corrective_action_id" value="${escapeHtml(ed.corrective_action_id)}">`
        : ""}
              <div class="dates-grid">
                <div class="form-group">
                  <label for="due_date">📅 Due Date *</label>
                  <input
                    type="date"
                    id="due_date"
                    name="due_date"
                    required
                    value="${defaultDueDate}"
                  >
                  <div class="hint">Set the final due date for this corrective action.</div>
                </div>

                <div class="form-group">
                  <label for="responsible">👤 Responsible *</label>
                  <input
                    type="text"
                    id="responsible"
                    name="responsible"
                    required
                    value="${defaultResponsibleName}"
                  >
                </div>
              </div>

<div class="form-group">
  <label for="root_cause">🔍 Root Cause Analysis *</label>
  <textarea id="root_cause" name="root_cause" required>${escapeHtml(ed.root_cause || "")}</textarea>
</div>

<div class="form-group">
  <label for="implemented_solution">🔧 Implemented Solution *</label>
  <textarea id="implemented_solution" name="implemented_solution" required>${escapeHtml(ed.implemented_solution || "")}</textarea>
</div>


              <div class="actions">
                <button type="submit" class="submit-btn">${submitLabel}</button>
                <a class="back-btn"
                   href="/corrective-action-form?responsible_id=${encodeURIComponent(responsible_id)}&kpi_id=${encodeURIComponent(kpi_id)}&week=${encodeURIComponent(week)}&new_action=1">
                  Add New Action
                </a>
                <a class="back-btn"
                   href="/corrective-actions-list?responsible_id=${encodeURIComponent(responsible_id)}">
                  Back to Corrective Actions
                </a>
              </div>
            </form>
          </div>
        </div>

        <script>
          const responsibleInput = document.getElementById('responsible');
          if (responsibleInput) {
            responsibleInput.addEventListener('blur', () => {
              responsibleInput.value = responsibleInput.value.trim();
            });
          }
        </script>
      </body>
      </html>
    `);
  } catch (err) {
    console.error("Error loading corrective action form:", err);
    res.status(500).send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});

app.post("/submit-corrective-action", async (req, res) => {
  try {
    const {
      responsible_id,
      kpi_id,
      week,
      corrective_action_id,
      root_cause,
      implemented_solution,
      due_date,
      responsible
    } = req.body || {};

    if (!responsible_id || !kpi_id || !week) {
      return res.status(400).send("Missing responsible_id, kpi_id, or week");
    }

    const saved = await upsertCorrectiveAction(responsible_id, kpi_id, week, {
      correctiveActionId: normalizeText(corrective_action_id),
      rootCause: normalizeText(root_cause),
      implementedSolution: normalizeText(implemented_solution),
      dueDate: due_date || null,
      responsibleName: normalizeText(responsible),
      status: "Open"
    });

    if (!saved) {
      return res.status(500).send("Could not save corrective action");
    }

    res.redirect(`/corrective-actions-list?responsible_id=${encodeURIComponent(responsible_id)}`);
  } catch (err) {
    console.error("Error submitting corrective action:", err);
    res.status(500).send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});

app.get("/corrective-actions-bulk", async (req, res) => {
  try {
    const { responsible_id, week } = req.query;

    const resResp = await pool.query(
      `SELECT r.responsible_id, r.name, r.email, r.plant_id, r.department_id,
              p.name AS plant_name, d.name AS department_name
       FROM public."Responsible" r
       JOIN public."Plant" p ON r.plant_id = p.plant_id
       JOIN public."Department" d ON r.department_id = d.department_id
       WHERE r.responsible_id = $1`,
      [responsible_id]
    );
    const responsible = resResp.rows[0];
    if (!responsible) return res.status(404).send("Responsible not found");

    const actionsRes = await pool.query(
      `SELECT ca.*, k.kpi_id, k.subject AS indicator_title, k.indicator_sub_title,
              k.unit, k.target, kv.value
       FROM public.corrective_actions ca
       JOIN public."Kpi" k ON ca.kpi_id = k.kpi_id
       LEFT JOIN public.kpi_values kv
         ON ca.kpi_id = kv.kpi_id
        AND kv.responsible_id = ca.responsible_id
        AND kv.week = ca.week
       WHERE ca.responsible_id = $1 AND ca.week = $2 AND ca.status = 'Open'
       ORDER BY k.subject`,
      [responsible_id, week]
    );
    const actions = actionsRes.rows;

    if (actions.length === 0) {
      return res.send(`
        <div style="text-align:center;padding:60px;font-family:'Segoe UI',sans-serif;">
          <h2 style="color:#4caf50;">✅ No Open Corrective Actions</h2>
          <p>All corrective actions for week ${week} have been completed.</p>
          <a href="/dashboard?responsible_id=${responsible_id}"
             style="display:inline-block;padding:12px 25px;background:#0078D7;color:white;
                    text-decoration:none;border-radius:6px;font-weight:bold;">
            Go to Dashboard
          </a>
        </div>`);
    }

    const kpiSectionsHtml = actions.map((action, index) => {
      const gap = action.target
        ? (parseFloat(action.target) - parseFloat(action.value || 0)).toFixed(2)
        : null;
      const pctGap = action.target && action.value
        ? (((parseFloat(action.target) - parseFloat(action.value)) / parseFloat(action.target)) * 100).toFixed(1)
        : null;
      const defaultDueDate = formatInputDate(action.due_date);
      const defaultResponsibleName = action.responsible || responsible.name || "";
      const safeResponsibleName = String(defaultResponsibleName)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");

      return `
        <div class="kpi-section">
          <input type="hidden" name="corrective_action_ids[]" value="${action.corrective_action_id}">

          <div class="kpi-header">
            <div class="kpi-number">${index + 1}</div>
            <div class="kpi-info">
              <div class="kpi-title">${action.indicator_title}</div>
              ${action.indicator_sub_title ? `<div class="kpi-subtitle">${action.indicator_sub_title}</div>` : ''}
            </div>
          </div>

          <div class="perf-stats">
            <div class="stat-box stat-current">
              <div class="stat-label">Current Value</div>
              <div class="stat-value">${action.value || '0'} <span class="stat-unit">${action.unit || ''}</span></div>
            </div>
            <div class="stat-box stat-target">
              <div class="stat-label">Target</div>
              <div class="stat-value">${action.target || 'N/A'} <span class="stat-unit">${action.unit || ''}</span></div>
            </div>
            <div class="stat-box stat-gap">
              <div class="stat-label">Gap</div>
              <div class="stat-value">${gap || 'N/A'} <span class="stat-unit">${action.unit || ''}</span></div>
              ${pctGap ? `<div class="stat-pct">${pctGap}% below target</div>` : ''}
            </div>
          </div>

          <!-- AI Box -->
          <div class="ai-box" id="ai-box-${action.corrective_action_id}">
            <div class="ai-box-header">
              <span class="ai-icon">🤖</span>
              <span class="ai-title">AI Corrective Action Suggestion</span>
              <button type="button" class="generate-btn"
                id="gen-btn-${action.corrective_action_id}"
                onclick="generateSuggestion('${action.corrective_action_id}','${action.kpi_id}','${responsible_id}','${week}')">
                <span class="gen-btn-icon">✨</span>
                <span class="gen-btn-text">Generate Suggestion</span>
              </button>
            </div>

            <div class="suggestion-content" id="suggestion-${action.corrective_action_id}" style="display:none;">
              <div class="ai-suggestion-row">
                <div class="ai-suggestion-card root-cause-card"
                     onclick="applyToField('root_cause_${action.corrective_action_id}',this)">
                  <div class="ai-card-label">
                    <span class="ai-card-icon">🔍</span>Root Cause
                    <span class="apply-hint">Click to apply ↓</span>
                  </div>
                  <div class="ai-card-text" id="rc-text-${action.corrective_action_id}"></div>
                </div>
                <div class="ai-suggestion-card action-card"
                     onclick="applyToField('solution_${action.corrective_action_id}',this)">
                  <div class="ai-card-label">
                    <span class="ai-card-icon">⚡</span>Immediate Action
                    <span class="apply-hint">Click to apply ↓</span>
                  </div>
                  <div class="ai-card-text" id="ia-text-${action.corrective_action_id}"></div>
                </div>
                <div class="ai-suggestion-card evidence-card"
                     onclick="applyToField('evidence_${action.corrective_action_id}',this)">
                  <div class="ai-card-label">
                    <span class="ai-card-icon">📊</span>Evidence
                    <span class="apply-hint">Click to apply ↓</span>
                  </div>
                  <div class="ai-card-text" id="ev-text-${action.corrective_action_id}"></div>
                </div>
              </div>
            </div>

            <div class="suggestion-error" id="error-${action.corrective_action_id}" style="display:none;">
              <span>⚠️ Could not generate suggestion. Please try again or fill manually.</span>
            </div>
          </div>

          <!-- Form Fields -->
          <div class="form-fields">
            <div class="form-group">
              <label for="root_cause_${action.corrective_action_id}">
                🔍 Root Cause <span class="required">*</span>
              </label>
              <textarea name="root_cause_${action.corrective_action_id}"
                        id="root_cause_${action.corrective_action_id}" required
                        placeholder="Click 'Generate Suggestion' above, or describe the root cause manually"
              >${action.root_cause || ''}</textarea>
            </div>
            <div class="form-group">
              <label for="solution_${action.corrective_action_id}">
                ⚡ Implemented Solution <span class="required">*</span>
              </label>
              <textarea name="solution_${action.corrective_action_id}"
                        id="solution_${action.corrective_action_id}" required
                        placeholder="Click 'Generate Suggestion' above, or describe actions taken manually"
              >${action.implemented_solution || ''}</textarea>
            </div>
            <div class="form-group">
              <label for="evidence_${action.corrective_action_id}">
                📊 Evidence <span class="required">*</span>
              </label>
              <textarea name="evidence_${action.corrective_action_id}"
                        id="evidence_${action.corrective_action_id}" required
                        placeholder="What evidence shows improvement?"
              >${action.evidence || ''}</textarea>
              <div class="help-text">Provide data, metrics, or observations demonstrating effectiveness</div>
            </div>

            <div class="form-grid">
              <div class="form-group">
                <label for="due_date_${action.corrective_action_id}">
                  📅 Due Date <span class="required">*</span>
                </label>
                <input
                  type="date"
                  name="due_date_${action.corrective_action_id}"
                  id="due_date_${action.corrective_action_id}"
                  value="${defaultDueDate}"
                  required
                >
              </div>

              <div class="form-group">
                <label for="responsible_${action.corrective_action_id}">
                  👤 Responsible <span class="required">*</span>
                </label>
                <input
                  type="text"
                  name="responsible_${action.corrective_action_id}"
                  id="responsible_${action.corrective_action_id}"
                  value="${safeResponsibleName}"
                  required
                >
              </div>
            </div>
          </div>
        </div>`;
    }).join('');

    res.send(`
      <!DOCTYPE html>
      <html lang="en">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1.0">
        <title>Corrective Actions — Week ${week}</title>
        <style>
          *,*::before,*::after{box-sizing:border-box;}
          body{font-family:'Segoe UI',system-ui,sans-serif;margin:0;padding:24px 16px;min-height:100vh;
            background:#f0f2f5;
            background-image:url("https://images.unsplash.com/photo-1486262715619-67b85e0b08d3?w=1600");
            background-size:cover;background-position:center;background-attachment:fixed;}
          .container{max-width:960px;margin:0 auto;background:rgba(255,255,255,0.97);border-radius:12px;
            box-shadow:0 8px 32px rgba(0,0,0,0.18);overflow:hidden;}
          .header{background:linear-gradient(135deg,#b71c1c 0%,#e53935 100%);color:white;
            padding:32px 28px;text-align:center;}
          .header-icon{font-size:52px;margin-bottom:12px;}
          .header h1{margin:0 0 8px;font-size:26px;font-weight:700;}
          .header-badge{display:inline-block;background:rgba(255,255,255,0.2);padding:6px 18px;
            border-radius:20px;font-size:13px;margin-top:6px;}
          .responsible-bar{background:rgba(0,0,0,0.15);padding:10px 20px;font-size:13px;text-align:center;}
          .form-section{padding:28px;}
          .kpi-section{background:#fff;border:1.5px solid #e5e7eb;border-radius:12px;
            margin-bottom:32px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.06);}
          .kpi-header{display:flex;align-items:center;gap:14px;padding:18px 20px;
            background:#fef2f2;border-bottom:1.5px solid #fecaca;}
          .kpi-number{flex-shrink:0;width:36px;height:36px;background:#dc2626;color:white;
            border-radius:50%;display:flex;align-items:center;justify-content:center;
            font-weight:700;font-size:15px;}
          .kpi-title{font-size:16px;font-weight:700;color:#111827;}
          .kpi-subtitle{font-size:12px;color:#6b7280;margin-top:3px;}
          .perf-stats{display:grid;grid-template-columns:repeat(3,1fr);border-bottom:1.5px solid #f3f4f6;}
          .stat-box{text-align:center;padding:16px 12px;}
          .stat-box+.stat-box{border-left:1px solid #f3f4f6;}
          .stat-label{font-size:10px;font-weight:600;text-transform:uppercase;color:#9ca3af;
            letter-spacing:0.6px;margin-bottom:6px;}
          .stat-value{font-size:22px;font-weight:800;}
          .stat-unit{font-size:13px;font-weight:500;}
          .stat-pct{font-size:11px;margin-top:4px;font-weight:600;}
          .stat-current .stat-value{color:#dc2626;}
          .stat-target .stat-value{color:#16a34a;}
          .stat-gap .stat-value{color:#d97706;}
          .stat-gap .stat-pct{color:#d97706;}
          /* AI Box */
          .ai-box{margin:20px 20px 0;border:1.5px solid #c4b5fd;border-radius:10px;
            background:linear-gradient(135deg,#f5f3ff 0%,#ede9fe 100%);overflow:hidden;}
          .ai-box-header{display:flex;align-items:center;gap:8px;padding:12px 16px;
            background:rgba(109,40,217,0.08);border-bottom:1px solid #ddd6fe;}
          .ai-icon{font-size:18px;}
          .ai-title{font-size:14px;font-weight:700;color:#5b21b6;flex:1;}
          .generate-btn{display:inline-flex;align-items:center;gap:6px;padding:8px 16px;
            background:linear-gradient(135deg,#7c3aed,#6d28d9);color:white;border:none;
            border-radius:20px;font-size:13px;font-weight:600;cursor:pointer;
            transition:all 0.2s;box-shadow:0 2px 8px rgba(109,40,217,0.35);}
          .generate-btn:hover:not(:disabled){background:linear-gradient(135deg,#6d28d9,#5b21b6);
            transform:translateY(-1px);box-shadow:0 4px 12px rgba(109,40,217,0.45);}
          .generate-btn:disabled{opacity:0.7;cursor:not-allowed;transform:none;}
          .generate-btn.loading .gen-btn-icon{animation:spin 1s linear infinite;display:inline-block;}
          @keyframes spin{to{transform:rotate(360deg);}}
          @keyframes fadeIn{from{opacity:0;transform:translateY(-8px);}to{opacity:1;transform:translateY(0);}}
          .suggestion-content{padding:14px 16px;animation:fadeIn 0.3s ease;}
          .suggestion-error{padding:12px 16px;font-size:13px;color:#92400e;background:#fff7ed;}
          .ai-suggestion-row{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;}
          .ai-suggestion-card{background:white;border-radius:8px;padding:14px;cursor:pointer;
            transition:transform 0.15s,box-shadow 0.15s;border:1.5px solid transparent;}
          .ai-suggestion-card:hover{transform:translateY(-2px);box-shadow:0 6px 16px rgba(0,0,0,0.1);}
          .ai-suggestion-card.applied{border-color:#4ade80!important;background:#f0fdf4;}
          .root-cause-card{border-top:3px solid #ef4444;}
          .action-card{border-top:3px solid #f59e0b;}
          .evidence-card{border-top:3px solid #3b82f6;}
          .ai-card-label{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;
            margin-bottom:8px;display:flex;align-items:center;gap:5px;}
          .root-cause-card .ai-card-label{color:#dc2626;}
          .action-card .ai-card-label{color:#d97706;}
          .evidence-card .ai-card-label{color:#2563eb;}
          .apply-hint{margin-left:auto;font-size:10px;font-weight:500;color:#9ca3af;
            text-transform:none;letter-spacing:0;}
          .ai-card-text{font-size:13px;color:#374151;line-height:1.55;}
          /* Form fields */
          .form-fields{padding:20px;}
          .form-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:14px;}
          .form-group{margin-bottom:18px;}
          label{display:block;font-size:13px;font-weight:600;color:#374151;margin-bottom:6px;}
          .required{color:#dc2626;margin-left:3px;}
          textarea,input[type="date"],input[type="text"]{width:100%;padding:11px 14px;border:1.5px solid #d1d5db;border-radius:6px;
            font-size:13px;font-family:inherit;resize:vertical;min-height:80px;
            transition:border-color 0.2s;}
          input[type="date"],input[type="text"]{min-height:auto;}
          textarea:focus,input[type="date"]:focus,input[type="text"]:focus{border-color:#7c3aed;outline:none;
            box-shadow:0 0 0 3px rgba(124,58,237,0.1);}
          textarea.highlight{animation:highlightFade 1.8s forwards;}
          @keyframes highlightFade{
            0%{background:#dcfce7;border-color:#16a34a;}
            100%{background:white;border-color:#d1d5db;}}
          .help-text{font-size:11px;color:#6b7280;margin-top:5px;}
          .submit-btn{width:100%;padding:16px;background:linear-gradient(135deg,#b71c1c,#e53935);
            color:white;border:none;border-radius:8px;font-size:17px;font-weight:700;
            cursor:pointer;margin-top:8px;transition:opacity 0.15s;}
          .submit-btn:hover{opacity:0.9;}
          @media(max-width:640px){.ai-suggestion-row,.form-grid{grid-template-columns:1fr;}}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <div class="header-icon">⚠️</div>
            <h1>Corrective Actions Required</h1>
            <div class="header-badge">
              ${actions.length} KPI${actions.length > 1 ? 's' : ''} Below Target — Week ${week}
            </div>
          </div>
          <div class="responsible-bar">
            👤 <strong>${responsible.name}</strong> &nbsp;•&nbsp;
            🏭 ${responsible.plant_name} &nbsp;•&nbsp;
            🏷️ ${responsible.department_name}
          </div>

          <div class="form-section">
            <form action="/submit-bulk-corrective-actions" method="POST">
              <input type="hidden" name="responsible_id" value="${responsible_id}">
              <input type="hidden" name="week" value="${week}">
              ${kpiSectionsHtml}
              <button type="submit" class="submit-btn">
                ✓ Submit All Corrective Actions (${actions.length})
              </button>
            </form>
          </div>
        </div>

        <script>
          function applyToField(fieldId, card) {
            const text = card.querySelector('.ai-card-text').textContent.trim();
            const field = document.getElementById(fieldId);
            if (!field || !text) return;
            field.value = text;
            field.classList.remove('highlight');
            void field.offsetWidth;
            field.classList.add('highlight');
            field.scrollIntoView({ behavior:'smooth', block:'center' });
            card.classList.add('applied');
            const hint = card.querySelector('.apply-hint');
            if (hint) hint.textContent = '✓ Applied';
          }

          async function generateSuggestion(caId, kpiId, responsibleId, week) {
            const btn         = document.getElementById('gen-btn-' + caId);
            const suggDiv     = document.getElementById('suggestion-' + caId);
            const errDiv      = document.getElementById('error-' + caId);

            suggDiv.style.display = 'none';
            errDiv.style.display  = 'none';

            suggDiv.querySelectorAll('.ai-suggestion-card').forEach(c => {
              c.classList.remove('applied');
              const hint = c.querySelector('.apply-hint');
              if (hint) hint.textContent = 'Click to apply ↓';
            });

            btn.disabled = true;
            btn.classList.add('loading');
            btn.querySelector('.gen-btn-icon').textContent = '⏳';
            btn.querySelector('.gen-btn-text').textContent = 'Generating...';

            try {
              const res = await fetch(
                '/generate-ca-suggestion?kpi_id=' + kpiId +
                '&responsible_id=' + responsibleId +
                '&week=' + encodeURIComponent(week)
              );
              if (!res.ok) throw new Error('Request failed');
              const data = await res.json();
              if (data.error || !data.suggestion) throw new Error(data.error || 'No suggestion');

              const s = data.suggestion;
              document.getElementById('rc-text-' + caId).textContent = s.root_cause       || '';
              document.getElementById('ia-text-' + caId).textContent = s.immediate_action || '';
              document.getElementById('ev-text-' + caId).textContent = s.evidence         || '';
              suggDiv.style.display = 'block';
            } catch (err) {
              errDiv.style.display = 'block';
            } finally {
              btn.disabled = false;
              btn.classList.remove('loading');
              btn.querySelector('.gen-btn-icon').textContent = '🔄';
              btn.querySelector('.gen-btn-text').textContent = 'Regenerate';
            }
          }
        </script>
      </body>
      </html>
    `);
  } catch (err) {
    console.error("Error loading bulk corrective actions:", err);
    res.status(500).send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});

app.post("/submit-bulk-corrective-actions", async (req, res) => {
  try {
    const { responsible_id, week, corrective_action_ids, ...formData } = req.body;
    const ids = Array.isArray(corrective_action_ids) ? corrective_action_ids : [corrective_action_ids];
    let completedCount = 0;
    for (const caId of ids) {
      const rootCause = normalizeText(formData[`root_cause_${caId}`]);
      const solution = normalizeText(formData[`solution_${caId}`]);
      const evidence = normalizeText(formData[`evidence_${caId}`]);
      const dueDate = formData[`due_date_${caId}`] || null;
      const responsibleName = normalizeText(formData[`responsible_${caId}`]);
      if (rootCause || solution || evidence || dueDate || responsibleName) {
        await pool.query(
          `UPDATE public.corrective_actions
           SET root_cause=$1,
               implemented_solution=$2,
               evidence=$3,
               due_date=$4::date,
               responsible=$5::text,
               status = CASE
                 WHEN $1::text IS NOT NULL
                  AND $2::text IS NOT NULL
                  AND $3::text IS NOT NULL
                  AND $4::date IS NOT NULL
                  AND NULLIF(BTRIM($5::text), '') IS NOT NULL
                 THEN 'Waiting for validation'
                 ELSE status
               END,
               updated_date=NOW()
           WHERE corrective_action_id=$6`,
          [rootCause, solution, evidence, dueDate, responsibleName, caId]
        );
        if (rootCause && solution && evidence && dueDate && responsibleName) {
          completedCount++;
        }
      }
    }
    res.send(`
      <!DOCTYPE html><html>
      <head><meta charset="utf-8"><title>Corrective Actions Submitted</title>
      <style>body{font-family:'Segoe UI',sans-serif;background:#f4f6f9;display:flex;
        justify-content:center;align-items:center;height:100vh;margin:0;}
      .sc{background:white;padding:50px;border-radius:10px;box-shadow:0 4px 15px rgba(0,0,0,0.1);
          text-align:center;max-width:600px;}
      h1{color:#4caf50;font-size:32px;margin-bottom:20px;}
      .count{font-size:48px;font-weight:700;color:#4caf50;margin:20px 0;}
      p{font-size:16px;color:#333;margin-bottom:30px;line-height:1.6;}
      a{display:inline-block;padding:14px 30px;background:#0078D7;color:white;
        text-decoration:none;border-radius:6px;font-weight:bold;margin:5px;}
      a:hover{background:#005ea6;}</style></head>
      <body><div class="sc">
        <h1>✅ All Corrective Actions Submitted!</h1>
        <div class="count">${completedCount}</div>
        <p>You have successfully submitted all corrective actions for week ${week}.<br>
           The quality team will review your submissions.</p>
        <a href="/corrective-actions-list?responsible_id=${responsible_id}">View Corrective Actions</a>
      </div></body></html>`);
  } catch (err) {
    res.status(500).send(`<h2 style="color:red;">Error: ${err.message}</h2>`);
  }
});

// ---------- Redirect handler ----------
app.post("/redirect", async (req, res) => {
  try {
    const { responsible_id, week, ...values } = req.body;

    // Extract KPI values
    const kpiValues = Object.entries(values)
      .filter(([k]) => k.startsWith("value_"))
      .map(([k, v]) => ({ kpi_values_id: k.split("_")[1], value: v }));

    // Extract comments
    const comments = Object.entries(values)
      .filter(([k]) => k.startsWith("comment_"))
      .reduce((acc, [k, v]) => { acc[k.split("_")[1]] = v; return acc; }, {});

    const responsibleRes = await pool.query(
      `SELECT name FROM public."Responsible" WHERE responsible_id = $1`,
      [responsible_id]
    );
    const defaultResponsibleName = responsibleRes.rows[0]?.name || null;

    const targetUpdates = [];
    let correctiveActionsCount = 0;

    for (let item of kpiValues) {
      const oldRes = await pool.query(
        `SELECT value, kpi_id FROM public."kpi_values" WHERE kpi_values_id = $1`,
        [item.kpi_values_id]
      );
      if (!oldRes.rows.length) continue;

      const { value: old_value, kpi_id } = oldRes.rows[0];

      // Fetch KPI limits to determine if corrective action is needed
      const kpiInfoRes = await pool.query(
        `SELECT kpi_id, subject, indicator_sub_title, target, low_limit, high_limit
         FROM public."Kpi"
         WHERE kpi_id = $1`,
        [kpi_id]
      );
      const kpiInfo = kpiInfoRes.rows[0] || {};
      const lowLimit = parseMetricNumber(kpiInfo.low_limit);
      const highLimit = parseMetricNumber(kpiInfo.high_limit);
      const goodDirection = inferKpiDirection(kpiInfo);
      const numValue = parseFloat(item.value);
      const submittedActions = getSubmittedCorrectiveActions(
        values,
        item.kpi_values_id,
        defaultResponsibleName
      );
      await pool.query(
        `INSERT INTO public.kpi_values_hist26
         (kpi_values_id, responsible_id, kpi_id, week, old_value, new_value, comment)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [item.kpi_values_id, responsible_id, kpi_id, week, old_value, item.value,
        comments[item.kpi_values_id] || null]
      );

      await pool.query(
        `UPDATE public."kpi_values" SET value = $1 WHERE kpi_values_id = $2`,
        [item.value, item.kpi_values_id]
      );

      // -------------------------------------------------------
      // Save corrective action if the KPI is outside the good-direction limit
      // and the user filled in the corrective action fields
      // -------------------------------------------------------
      const meaningfulActions = submittedActions.filter(hasMeaningfulCorrectiveActionInput);

      if (meaningfulActions.length) {
        for (const action of meaningfulActions) {
          await upsertCorrectiveAction(responsible_id, kpi_id, week, action);
          correctiveActionsCount++;
        }
      } else if (
        !isNaN(numValue) &&
        needsCorrectiveAction(numValue, lowLimit, highLimit, goodDirection)
      ) {
        const existing = await pool.query(
          `SELECT corrective_action_id
     FROM public.corrective_actions
     WHERE responsible_id = $1 AND kpi_id = $2 AND week = $3
     LIMIT 1`,
          [responsible_id, kpi_id, week]
        );

        if (existing.rows.length === 0) {
          await pool.query(
            `INSERT INTO public.corrective_actions
       (responsible_id, kpi_id, week, status, due_date, responsible)
       VALUES ($1, $2, $3, 'Open', CURRENT_DATE + 7, $4)`,
            [responsible_id, kpi_id, week, normalizeText(defaultResponsibleName)]
          );
        }
      }

      const histRes = await pool.query(
        `SELECT hist_id FROM public.kpi_values_hist26
         WHERE kpi_values_id=$1 AND responsible_id=$2 AND kpi_id=$3 AND week=$4
         ORDER BY updated_at DESC LIMIT 1`,
        [item.kpi_values_id, responsible_id, kpi_id, week]
      );
      if (histRes.rows.length > 0) {
        const result = await checkAndTriggerCorrectiveActions(
          responsible_id, kpi_id, week, item.value, histRes.rows[0].hist_id
        );
        if (result.targetUpdated && result.updateInfo) targetUpdates.push(result.updateInfo);
      }
    }

    let notifications = [];
    if (targetUpdates.length > 0)
      notifications.push(`🎯 <strong>${targetUpdates.length} KPI target${targetUpdates.length > 1 ? 's' : ''} updated</strong>`);
    if (correctiveActionsCount > 0)
      notifications.push(`📋 <strong>${correctiveActionsCount} corrective action${correctiveActionsCount > 1 ? 's' : ''} recorded</strong>`);
    if (notifications.length === 0) notifications.push(`📊 All KPIs are within targets`);

    res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>KPI Submitted</title>
      <style>body{font-family:'Segoe UI',sans-serif;background:#f4f4f4;display:flex;
        justify-content:center;align-items:center;height:100vh;margin:0;}
      .sc{background:#fff;padding:40px;border-radius:10px;text-align:center;max-width:600px;}
      .ni{display:flex;align-items:center;margin:10px 0;padding:10px;background:white;border-radius:6px;}
      .btn{display:inline-block;padding:12px 25px;background:#0078D7;color:white;
           text-decoration:none;border-radius:6px;font-weight:bold;margin:5px;}</style></head>
      <body><div class="sc">
        <h1 style="color:#28a745;">✅ KPI Submitted Successfully!</h1>
        <p>Your KPI values for ${week} have been saved.</p>
        <div style="background:#f8f9fa;padding:20px;border-radius:8px;margin:20px 0;">
          ${notifications.map(n => `<div class="ni"><span style="margin-right:10px;">📌</span><span>${n}</span></div>`).join('')}
        </div>
        <a href="/dashboard?responsible_id=${responsible_id}" class="btn">Go to Dashboard</a>
      </div></body></html>`);
  } catch (err) {
    res.status(500).send(`<h2 style="color:red;">❌ Failed: ${err.message}</h2>`);
  }
});



app.get("/api/kpi-chart-data", async (req, res) => {
  try {
    const { responsible_id, kpi_id, week } = req.query;

    if (!responsible_id || !kpi_id || !week) {
      return res.status(400).json({ error: "Missing responsible_id, kpi_id, or week" });
    }

    const histRes = await pool.query(
      `
      SELECT DISTINCT ON (week)
        week,
        new_value,
        updated_at
      FROM public.kpi_values_hist26
      WHERE responsible_id = $1
        AND kpi_id = $2
        AND new_value IS NOT NULL
        AND new_value <> ''
      ORDER BY week, updated_at DESC
      `,
      [responsible_id, kpi_id]
    );

    const currentRes = await pool.query(
      `
      SELECT kv.value
      FROM public.kpi_values kv
      WHERE kv.responsible_id = $1
        AND kv.kpi_id = $2
        AND kv.week = $3
      LIMIT 1
      `,
      [responsible_id, kpi_id, week]
    );

    function weekLabelToDate(weekStr) {
      const m = String(weekStr || "").match(/^(\d{4})-Week(\d{1,2})$/);
      if (!m) return new Date(0);
      const year = parseInt(m[1], 10);
      const weekNum = parseInt(m[2], 10);
      return new Date(year, 0, 1 + (weekNum - 1) * 7);
    }

    function weekToMonthLabel(weekStr) {
      const d = weekLabelToDate(weekStr);
      if (isNaN(d.getTime())) return weekStr || "";
      return d.toLocaleString("en-US", { month: "short", year: "numeric" });
    }

    function monthLabelToDate(monthLabel) {
      const d = new Date("1 " + monthLabel);
      return isNaN(d.getTime()) ? new Date(0) : d;
    }

    const monthMap = {};
    for (const row of histRes.rows) {
      const value = parseFloat(row.new_value);
      if (isNaN(value)) continue;

      const monthLabel = weekToMonthLabel(row.week);
      if (!monthMap[monthLabel]) monthMap[monthLabel] = { sum: 0, count: 0 };
      monthMap[monthLabel].sum += value;
      monthMap[monthLabel].count += 1;
    }

    const currentMonthLabel = weekToMonthLabel(week);
    const currentValue = parseFloat(currentRes.rows[0]?.value);

    if (!isNaN(currentValue)) {
      monthMap[currentMonthLabel] = { sum: currentValue, count: 1 };
    }

    const labels = Object.keys(monthMap).sort(
      (a, b) => monthLabelToDate(a) - monthLabelToDate(b)
    );

    const values = labels.map((label) => {
      const m = monthMap[label];
      return Number((m.sum / m.count).toFixed(2));
    });

    res.json({ labels, values, currentMonthLabel, currentValue: isNaN(currentValue) ? null : currentValue });
  } catch (err) {
    console.error("kpi-chart-data error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ---------- Form page ----------
app.get("/form", async (req, res) => {
  try {
    const { responsible_id, week } = req.query;
    const { responsible, kpis } = await getResponsibleWithKPIs(responsible_id, week);

    if (!kpis.length) {
      return res.send("<p>No KPIs found for this week.</p>");
    }

    const histRes = await pool.query(
      `
      SELECT DISTINCT ON (kpi_id, week)
        kpi_id,
        week,
        new_value,
        updated_at
      FROM public.kpi_values_hist26
      WHERE responsible_id = $1
        AND new_value IS NOT NULL
        AND new_value <> ''
      ORDER BY kpi_id, week, updated_at DESC
      `,
      [responsible_id]
    );

    const historyByKpi = {};
    histRes.rows.forEach((row) => {
      if (!historyByKpi[row.kpi_id]) historyByKpi[row.kpi_id] = [];
      historyByKpi[row.kpi_id].push({
        week: row.week,
        value: parseFloat(row.new_value)
      });
    });

    function weekLabelToDate(weekStr) {
      const m = String(weekStr || "").match(/^(\d{4})-Week(\d{1,2})$/);
      if (!m) return new Date(0);
      const year = parseInt(m[1], 10);
      const weekNum = parseInt(m[2], 10);
      return new Date(year, 0, 1 + (weekNum - 1) * 7);
    }

    function weekToMonthLabel(weekStr) {
      const d = weekLabelToDate(weekStr);
      if (isNaN(d.getTime())) return weekStr || "";
      return d.toLocaleString("en-US", { month: "short", year: "numeric" });
    }

    function monthLabelToDate(monthLabel) {
      const d = new Date("1 " + monthLabel);
      return isNaN(d.getTime()) ? new Date(0) : d;
    }

    function findPreviousMonthLabel(currentMonthLabel, labels) {
      const currentDate = monthLabelToDate(currentMonthLabel);
      return Array.from(new Set((labels || []).filter(Boolean)))
        .sort((a, b) => monthLabelToDate(a) - monthLabelToDate(b))
        .filter((label) => monthLabelToDate(label) < currentDate)
        .pop() || null;
    }

    const encodeModalPayload = (value) =>
      encodeURIComponent(JSON.stringify(value ?? null));

    const commentsHistoryRes = await pool.query(
      `
      SELECT DISTINCT ON (kpi_id, week)
        kpi_id,
        week,
        comment,
        updated_at
      FROM public.kpi_values_hist26
      WHERE responsible_id = $1
        AND comment IS NOT NULL
        AND BTRIM(comment) <> ''
      ORDER BY kpi_id, week, updated_at DESC
      `,
      [responsible_id]
    );

    const correctiveActionsHistoryRes = await pool.query(
      `
      SELECT
        kpi_id,
        week,
        root_cause,
        implemented_solution,
        evidence,
        status,
        due_date,
        responsible,
        created_date,
        updated_date
      FROM public.corrective_actions
      WHERE responsible_id = $1
      ORDER BY kpi_id ASC, week ASC, COALESCE(updated_date, created_date) DESC
      `,
      [responsible_id]
    );

    const commentsByKpiMonth = {};
    commentsHistoryRes.rows.forEach((row) => {
      const monthLabel = weekToMonthLabel(row.week);
      if (!commentsByKpiMonth[row.kpi_id]) commentsByKpiMonth[row.kpi_id] = {};
      if (!commentsByKpiMonth[row.kpi_id][monthLabel]) commentsByKpiMonth[row.kpi_id][monthLabel] = [];
      commentsByKpiMonth[row.kpi_id][monthLabel].push({
        week: row.week,
        month_label: monthLabel,
        text: row.comment,
        updated_at: row.updated_at
      });
    });

    const correctiveActionsByKpiMonth = {};
    correctiveActionsHistoryRes.rows.forEach((row) => {
      const monthLabel = weekToMonthLabel(row.week);
      const updatedAt = row.updated_date || row.created_date || null;

      if (!correctiveActionsByKpiMonth[row.kpi_id]) {
        correctiveActionsByKpiMonth[row.kpi_id] = {};
      }

      if (!correctiveActionsByKpiMonth[row.kpi_id][monthLabel]) {
        correctiveActionsByKpiMonth[row.kpi_id][monthLabel] = [];
      }

      correctiveActionsByKpiMonth[row.kpi_id][monthLabel].push({
        week: row.week,
        month_label: monthLabel,
        status: row.status || "",
        root_cause: row.root_cause || "",
        implemented_solution: row.implemented_solution || "",
        evidence: row.evidence || "",
        due_date: row.due_date || "",
        responsible: row.responsible || "",
        updated_at: updatedAt
      });
    });

    // optional: sort newest first
    Object.keys(correctiveActionsByKpiMonth).forEach((kpiId) => {
      Object.keys(correctiveActionsByKpiMonth[kpiId]).forEach((monthLabel) => {
        correctiveActionsByKpiMonth[kpiId][monthLabel].sort((a, b) => {
          return new Date(b.updated_at || 0) - new Date(a.updated_at || 0);
        });
      });
    });

    let kpiCardsHtml = "";

    kpis.forEach((kpi) => {
      let lowLimit = null;
      if (kpi.low_limit && kpi.low_limit !== "None" && kpi.low_limit !== "null" && kpi.low_limit !== "" && !isNaN(parseFloat(kpi.low_limit))) {
        lowLimit = parseFloat(kpi.low_limit);
      }
      let highLimit = null;
      if (kpi.high_limit && kpi.high_limit !== "None" && kpi.high_limit !== "null" && kpi.high_limit !== "" && !isNaN(parseFloat(kpi.high_limit))) {
        highLimit = parseFloat(kpi.high_limit);
      }
      let targetValue = null;
      if (kpi.target && kpi.target !== "None" && kpi.target !== "null" && kpi.target !== "" && !isNaN(parseFloat(kpi.target))) {
        targetValue = parseFloat(kpi.target);
      }

      const currentValue = kpi.value && kpi.value !== "" && !isNaN(parseFloat(kpi.value))
        ? parseFloat(kpi.value) : null;

      const goodDirection = inferKpiDirection(kpi);
      const showCA = currentValue !== null &&
        needsCorrectiveAction(currentValue, lowLimit, highLimit, goodDirection);
      const correctiveActions = sortCorrectiveActions(
        Array.isArray(kpi.corrective_actions) ? kpi.corrective_actions : []
      );
      const latestCorrectiveAction = getLatestCorrectiveAction(correctiveActions);
      const caStatus = latestCorrectiveAction?.status || "";
      const safeCaStatusClass = String(caStatus || "").toLowerCase().replace(/\s+/g, "-");
      const correctiveActionsToRender = correctiveActions.length ? correctiveActions : [{
        responsible: responsible.name || ""
      }];
      const correctiveActionsHtml = correctiveActionsToRender
        .map((action, actionIndex) => buildCorrectiveActionEntryHtml({
          kpiValuesId: kpi.kpi_values_id,
          actionIndex,
          action,
          defaultResponsibleName: responsible.name || "",
          showRequired: showCA
        }))
        .join("");

      // ── Corrective actions section (no footer add-btn inside) ──


      const rawHistory = historyByKpi[kpi.kpi_id] || [];
      const sortedHistory = rawHistory
        .filter((h) => !isNaN(h.value))
        .sort((a, b) => weekLabelToDate(a.week) - weekLabelToDate(b.week));

      const currentMonthLabel = weekToMonthLabel(week);
      const monthMap = {};

      sortedHistory.forEach((item) => {
        const monthLabel = weekToMonthLabel(item.week);

        if (!monthMap[monthLabel]) {
          monthMap[monthLabel] = { sum: 0, count: 0 };
        }

        monthMap[monthLabel].sum += item.value;
        monthMap[monthLabel].count += 1;
      });

      // also force the current input value into the current month
      if (currentValue !== null) {
        monthMap[currentMonthLabel] = {
          sum: currentValue,
          count: 1
        };
      }

      let historyLabels = Object.keys(monthMap).sort(
        (a, b) => monthLabelToDate(a) - monthLabelToDate(b)
      );

      let historyValues = historyLabels.map((label) => {
        const m = monthMap[label];
        return Number((m.sum / m.count).toFixed(2));
      });

      const commentMonths = Object.keys(commentsByKpiMonth[kpi.kpi_id] || {});
      const correctiveActionMonths = Object.keys(correctiveActionsByKpiMonth[kpi.kpi_id] || {});
      const previousMonthLabel = findPreviousMonthLabel(
        currentMonthLabel,
        historyLabels.concat(commentMonths, correctiveActionMonths)
      );
      const previousMonthActions = previousMonthLabel
        ? correctiveActionsByKpiMonth[kpi.kpi_id]?.[previousMonthLabel] || []
        : [];
      const previousMonthComments = previousMonthLabel
        ? (commentsByKpiMonth[kpi.kpi_id]?.[previousMonthLabel] || [])
          .sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at))
        : [];
      const hasPreviousMonthDetails = Boolean(
        (previousMonthActions && previousMonthActions.length) ||
        (previousMonthComments && previousMonthComments.length)
      );

      kpiCardsHtml += `
       <div class="kpi-card"
        data-kpi-id="${kpi.kpi_id}"
       data-kpi-values-id="${kpi.kpi_values_id}"
       data-low-limit="${lowLimit !== null ? lowLimit : ""}"
       data-high-limit="${highLimit !== null ? highLimit : ""}"
       data-good-direction="${goodDirection}"
       data-target="${targetValue !== null ? targetValue : ""}"
       data-history-labels='${JSON.stringify(historyLabels)}'
       data-history-values='${JSON.stringify(historyValues)}'
       data-current-week="${week}"
       data-current-month-label="${currentMonthLabel}"
       data-unit="${kpi.unit || ""}"
      data-prev-month-label="${previousMonthLabel || ""}"
      data-prev-month-actions="${encodeModalPayload(previousMonthActions)}"
      data-prev-month-comments="${encodeModalPayload(previousMonthComments)}">

          <div class="kpi-header">
            <div>
              <div class="kpi-title">${kpi.subject}</div>
              ${kpi.indicator_sub_title ? `<div class="kpi-subtitle">${kpi.indicator_sub_title}</div>` : ""}
            </div>
          </div>

          <div class="kpi-split-layout">
            <div class="kpi-right-panel kpi-chart-trigger"
                 data-kpi-values-id="${kpi.kpi_values_id}"
                 role="button"
                 tabindex="0"
                 title="Click to expand chart"
                 aria-label="Expand KPI chart">
              <button
                type="button"
                class="chart-expand-btn"
                data-kpi-values-id="${kpi.kpi_values_id}"
                aria-label="Expand KPI chart"
                title="Expand chart">
                <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                  <path d="M9 3H3v6"></path>
                  <path d="M15 3h6v6"></path>
                  <path d="M21 15v6h-6"></path>
                  <path d="M9 21H3v-6"></path>
                </svg>
              </button>
              <canvas id="chart_${kpi.kpi_values_id}"></canvas>
            </div>

            <div class="kpi-left-panel">
              <div class="kpi-entry-card">
             

                <div class="kpi-input-stack">
                  <label class="kpi-side-label" for="value_${kpi.kpi_values_id}">Current Value</label>
                  <div class="kpi-input-shell ${kpi.unit ? "has-unit" : ""}">
                    <input
                      type="number"
                      step="any"
                      id="value_${kpi.kpi_values_id}"
                      name="value_${kpi.kpi_values_id}"
                      value="${kpi.value ?? ""}"
                      placeholder="Enter value"
                      class="kpi-input value-input"
                      data-kpi-values-id="${kpi.kpi_values_id}"
                      required
                    />
                    ${kpi.unit ? `<span class="kpi-input-unit">${kpi.unit}</span>` : ""}
                  </div>
                </div>
                <div>
                     <!-- ── Manager Comment (moved above action bar) ── -->
          <div class="comment-section">
            <div class="comment-label">
              Manager Comment <span style="font-size:11px;color:#888;">(Optional)</span>
            </div>
            <textarea
              name="comment_${kpi.kpi_values_id}"
              class="comment-input"
              placeholder="Add your comment..."
            >${kpi.latest_comment || ""}</textarea>
          </div>
                </div>

                <div class="kpi-history-panel ${hasPreviousMonthDetails ? "history-available" : "history-empty"}">
                  <div class="kpi-history-copy">
                 
                  </div>
                  <button
                    type="button"
                    class="view-ca-btn"
                    data-kpi-values-id="${kpi.kpi_values_id}">
                    <span>View Corrective Action</span>
                    <span class="view-ca-btn-icon">↗</span>
                  </button>
                </div>
                    <!-- ── Unified card action bar ── -->
        <div class="kpi-card-actions">
       <div class="kpi-card-actions-left">
       <button
        type="button"
        class="card-action-btn card-action-btn--ai"
        onclick="openAssistantForKpi('${kpi.kpi_values_id}')">
        <span class="ai-btn-glow"></span>
        <span class="ai-btn-shine"></span>
        <span class="ai-btn-icon">🤖</span>
        <span class="ai-btn-text">AI Support</span>
       </button>
  </div>

  <div class="kpi-card-actions-right">
    <button
      type="button"
      class="card-action-btn card-action-btn--primary open-ca-modal-btn"
      data-kpi-values-id="${kpi.kpi_values_id}">
      <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round">
        <path d="M10 4v12M4 10h12"/>
      </svg>
      Add Action
    </button>
  </div>
</div>
              </div>

              <div class="kpi-mini-stats">
                <div class="mini-stat-card">
                  <div class="mini-stat-label">HIGH LIMIT</div>
                  <div class="mini-stat-value high">${highLimit !== null ? highLimit : "—"}</div>
                  <div class="mini-stat-unit">${kpi.unit || ""}</div>
                </div>
                <div class="mini-stat-card">
                  <div class="mini-stat-label">TARGET</div>
                  <div class="mini-stat-value target">${targetValue !== null ? targetValue : "—"}</div>
                  <div class="mini-stat-unit">${kpi.unit || ""}</div>
                </div>
                <div class="mini-stat-card">
                  <div class="mini-stat-label">LOW LIMIT</div>
                  <div class="mini-stat-value low">${lowLimit !== null ? lowLimit : "—"}</div>
                  <div class="mini-stat-unit">${kpi.unit || ""}</div>
                </div>
              </div>
            </div>
          </div>
         <div class="ca-actions-stack" data-kpi-values-id="${kpi.kpi_values_id}" style="display:none;">
         ${correctiveActionsHtml}
           </div>
        </div>
      `;
    });

    res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <title>KPI Form - Week ${week}</title>
        <style>
          body{
            font-family:'Segoe UI',sans-serif;
            background:#f4f6f9;
            background-image:url('https://images.unsplash.com/photo-1486262715619-67b85e0b08d3?w=1600');
            background-size:cover;
            background-position:center;
            background-attachment:fixed;
            padding:20px;
            margin:0;
            min-height:100vh;
          }
          .container{
            max-width:1500px;
            width:96%;
            margin:0 auto;
            background:rgba(255,255,255,0.95);
            border-radius:8px;
            box-shadow:0 4px 20px rgba(0,0,0,0.2);
            overflow:hidden;
          }
          .header{
            background:#0078D7;
            color:white;
            padding:20px;
            text-align:center;
          }
          .form-section{padding:30px;}
          .info-section{
            background:#f8f9fa;
            padding:20px;
            border-radius:6px;
            margin-bottom:25px;
            border-left:4px solid #0078D7;
          }
          .info-row{display:flex;margin-bottom:15px;align-items:center;}
          .info-label{font-weight:600;color:#333;width:120px;font-size:14px;}
          .info-value{flex:1;padding:8px 12px;background:white;border:1px solid #ddd;border-radius:4px;}

          /* ── KPI Card ── */
          .kpi-card{
            background:#fff;
            border:1px solid #e1e5e9;
            border-radius:6px;
            padding:20px;
            margin-bottom:20px;
          }
          .kpi-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;}
          .kpi-title{font-weight:600;color:#333;font-size:15px;}
          .kpi-subtitle{color:#666;font-size:13px;margin-bottom:10px;}

          .kpi-split-layout{
            display:grid;
            grid-template-columns:1fr 380px;
            gap:20px;
            align-items:stretch;
            margin-top:16px;
          }
          .kpi-left-panel,.kpi-right-panel{
            background:#fafafa;
            border:1px solid #e5e7eb;
            border-radius:20px;
            padding:18px;
          }
          .kpi-left-panel{
            min-height:480px;
            display:flex;
            align-items:center;
            justify-content:center;
            position:relative;
            overflow:hidden;
            background:
              radial-gradient(circle at top right,rgba(37,99,235,0.14),transparent 34%),
              linear-gradient(180deg,#ffffff 0%,#f7fbff 52%,#f2f7fc 100%);
          }
          .kpi-left-panel::before{
            content:"";
            position:absolute;
            width:220px;height:220px;right:-90px;top:-90px;
            border-radius:50%;
            background:radial-gradient(circle,rgba(56,189,248,0.18) 0%,rgba(56,189,248,0) 72%);
            pointer-events:none;
          }

        .kpi-entry-card{
        padding:24px;
        border-radius:24px;
        background:rgba(255,255,255,0.96);
        border:1px solid #d8e3ee;
        box-shadow:0 20px 40px rgba(15,23,42,0.10);
        display:flex;
        flex-direction:column;
        gap:18px;

        /* 👇 ADD THIS */
        padding-bottom:28px;
        }
          .kpi-entry-top{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;}
          .kpi-entry-eyebrow{font-size:11px;font-weight:800;letter-spacing:1.1px;text-transform:uppercase;color:#64748b;}
          .kpi-entry-title{margin-top:6px;color:#0f172a;font-size:18px;font-weight:800;line-height:1.25;}
          .kpi-entry-unit{
            flex-shrink:0;padding:8px 12px;border-radius:999px;
            background:#eff6ff;color:#1d4ed8;font-size:12px;font-weight:800;
            border:1px solid #bfdbfe;white-space:nowrap;
          }
          .kpi-input-stack{display:flex;flex-direction:column;gap:8px;}
          .kpi-side-label{font-size:12px;font-weight:800;letter-spacing:0.9px;text-transform:uppercase;color:#475569;}
          .kpi-input-shell{position:relative;}
          .kpi-input{width:100%;padding:10px 12px;border:1px solid #ddd;border-radius:4px;font-size:14px;box-sizing:border-box;}
          .kpi-input:focus{border-color:#0078D7;outline:none;}
          .kpi-left-panel .kpi-input{
            width:100%;margin:0;min-height:58px;padding:16px 18px;
            border:1.5px solid #d6dee8;border-radius:16px;background:#ffffff;
            color:#0f172a;font-size:24px;font-weight:800;
            box-shadow:inset 0 1px 0 rgba(255,255,255,0.9);
          }
          .kpi-left-panel .kpi-input::placeholder{color:#94a3b8;font-size:16px;font-weight:600;}
          .kpi-input-shell.has-unit .kpi-input{padding-right:76px;}
          .kpi-input-unit{
            position:absolute;top:50%;right:14px;transform:translateY(-50%);
            padding:6px 10px;border-radius:999px;background:#f8fafc;
            border:1px solid #e2e8f0;color:#64748b;font-size:12px;font-weight:800;pointer-events:none;
          }
          .kpi-left-panel .kpi-input:focus{
            border-color:#3b82f6;outline:none;
            box-shadow:0 0 0 4px rgba(59,130,246,0.12),0 14px 30px rgba(59,130,246,0.10);
          }
        
          .kpi-history-panel{
           width:100%;
           margin:0;
           padding:14px 0 0;   /* ⬅️ remove side padding */
           border:none;        /* ⬅️ remove border */
           background:transparent; /* ⬅️ remove background */
           display:flex;
           flex-direction:column;
           gap:14px;
            }
          .kpi-history-panel.history-empty{border-color:#e2e8f0;background:linear-gradient(180deg,#fbfdff 0%,#f8fafc 100%);}
          .kpi-history-copy{display:flex;flex-direction:column;gap:6px;}
          .kpi-history-kicker{font-size:11px;font-weight:800;letter-spacing:1px;text-transform:uppercase;color:#64748b;}
          .kpi-history-title{font-size:18px;font-weight:800;line-height:1.2;color:#0f172a;}
          .view-ca-btn{
          width:100%;
          border:none;
          border-radius:14px;
          padding:14px 16px;
          display:flex;
          align-items:center;
          justify-content:space-between;
          gap:12px;

          background:linear-gradient(135deg,#0f6cbd 0%,#2894ff 100%);
          color:white;
          font-size:14px;
          font-weight:800;
          cursor:pointer;
         box-shadow:0 8px 18px rgba(15,108,189,0.18); /* ⬅️ softer */
          }
          .view-ca-btn:hover{transform:translateY(-2px);box-shadow:0 18px 34px rgba(15,108,189,0.26);}
          .view-ca-btn-icon{
            width:32px;height:32px;border-radius:999px;
            display:inline-flex;align-items:center;justify-content:center;
            background:rgba(255,255,255,0.18);font-size:16px;
          }
          .view-ca-note{font-size:13px;color:#526277;line-height:1.6;}
          .kpi-mini-stats{display:none;}
          .kpi-right-panel{
            min-height:280px;display:flex;align-items:center;justify-content:center;position:relative;
          }
          .kpi-chart-trigger{
            cursor:zoom-in;transition:border-color 0.2s ease,box-shadow 0.2s ease;
          }
          .kpi-chart-trigger:hover{border-color:#bfdbfe;box-shadow:0 12px 28px rgba(37,99,235,0.10);}
          .chart-expand-btn{
            position:absolute;top:14px;right:14px;width:42px;height:42px;
            border:none;border-radius:14px;
            display:inline-flex;align-items:center;justify-content:center;
            background:rgba(255,255,255,0.92);color:#1d4ed8;
            box-shadow:0 12px 24px rgba(15,23,42,0.12);
            cursor:pointer;z-index:2;
            transition:transform 0.18s ease,box-shadow 0.18s ease;
          }
          .chart-expand-btn:hover{transform:translateY(-1px);background:#eff6ff;}
          .chart-expand-btn svg{width:18px;height:18px;stroke:currentColor;stroke-width:2;fill:none;stroke-linecap:round;stroke-linejoin:round;}
          .kpi-right-panel canvas{width:100% !important;height:380px !important;display:block;}

          .history-table-wrap {
  overflow-x: auto;
  border: 1px solid #e5e7eb;
  border-radius: 16px;
  background: #fff;
}

.history-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

.history-table thead tr {
  background: #f8fafc;
}

.history-table th {
  padding: 12px 14px;
  text-align: left;
  font-size: 11px;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: #334155;
  border-bottom: 1px solid #e5e7eb;
  white-space: nowrap;
}

.history-table td {
  padding: 12px 14px;
  border-bottom: 1px solid #f1f5f9;
  vertical-align: top;
  color: #334155;
  line-height: 1.5;
}

.history-table tr:last-child td {
  border-bottom: none;
}

.history-table td pre {
  margin: 0;
  white-space: pre-wrap;
  font-family: inherit;
}

          /* ── CA Section ── */
          .ca-container{
            margin-top:16px;
            background:linear-gradient(135deg,#fff5f5,#fff8f0);
            border:2px solid #f28b82;
            border-radius:10px;
            display:none;overflow:hidden;
          }
          .ca-container.visible{display:block;}
          .ca-header{
            font-weight:700;color:#c62828;font-size:14px;
            padding:14px 16px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;
            background:rgba(211,47,47,0.05);
          }
          .ca-header-copy{flex:1 1 280px;}
          .ca-header-meta{display:flex;align-items:center;gap:8px;margin-left:auto;flex-wrap:wrap;}
          .ca-count-badge{font-size:11px;font-weight:700;padding:4px 10px;border-radius:999px;background:#fff;border:1px solid #fecaca;color:#b91c1c;}
          .ca-status-badge{font-size:11px;font-weight:600;padding:3px 10px;border-radius:12px;}
          .ca-status-open{background:#ffebee;color:#c62828;border:1px solid #ef9a9a;}
          .ca-status-waiting-for-validation{background:#fff3e0;color:#e65100;border:1px solid #ffcc02;}
          .ca-actions-note{padding:14px 14px 0;font-size:12px;color:#7f1d1d;}
          .ca-ai-box{margin:14px 14px 0;border:none;border-radius:10px;background:linear-gradient(135deg,#f5f3ff,#ede9fe);overflow:hidden;}
          .ca-suggestion-content{padding:12px 14px;}
          .ca-ai-row{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;}
          .ca-ai-card{background:white;border-radius:8px;padding:12px;cursor:pointer;transition:transform 0.15s,box-shadow 0.15s;border:1.5px solid transparent;}
          .ca-ai-card:hover{transform:translateY(-2px);box-shadow:0 4px 12px rgba(0,0,0,0.1);}
          .ca-ai-card.applied{border-color:#4ade80 !important;background:#f0fdf4;}
          .ca-rc-card{border-top:3px solid #ef4444;}
          .ca-sol-card{border-top:3px solid #f59e0b;}
          .ca-ev-card{border-top:3px solid #3b82f6;}
          .ca-ai-card-label{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:7px;display:flex;align-items:center;gap:5px;}
          .ca-rc-card .ca-ai-card-label{color:#dc2626;}
          .ca-sol-card .ca-ai-card-label{color:#d97706;}
          .ca-ev-card .ca-ai-card-label{color:#2563eb;}
          .ca-apply-hint{margin-left:auto;font-size:9px;font-weight:500;color:#9ca3af;text-transform:none;letter-spacing:0;}
          .ca-ai-card-text{font-size:12px;color:#374151;line-height:1.5;}
          .ca-sugg-error{padding:10px 14px;font-size:12px;color:#92400e;background:#fff7ed;}
          .ca-actions-stack{padding:14px;display:grid;gap:14px;}
          .ca-action-card{
            background:rgba(255,255,255,0.85);border:1px solid #f8b4b4;
            border-radius:14px;padding:14px 0 2px;
            box-shadow:0 8px 20px rgba(239,68,68,0.06);
          }
          .ca-action-head{
            display:flex;justify-content:space-between;align-items:center;gap:12px;
            padding:0 14px 12px;margin-bottom:12px;
            border-bottom:1px solid #fee2e2;flex-wrap:wrap;
          }
          .ca-action-title{font-size:14px;font-weight:900;color:#991b1b;}
          .ca-action-tools{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
          .ca-remove-btn{border:none;border-radius:999px;padding:8px 12px;background:#fee2e2;color:#b91c1c;font-size:11px;font-weight:800;cursor:pointer;}
          .ca-remove-btn.is-hidden{display:none;}
          .ca-dates-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;padding:0 14px 14px;align-items:start;}
          .ca-dates-grid .ca-field{margin-top:0 !important;margin-bottom:0;padding:0;}
          .ca-date-input,.ca-text-input{width:100%;padding:10px 12px;border:1.5px solid #f28b82;border-radius:6px;font-size:13px;font-family:inherit;background:#fff;box-sizing:border-box;}
          .ca-date-input:focus,.ca-text-input:focus{border-color:#d32f2f;outline:none;box-shadow:0 0 0 3px rgba(211,47,47,0.12);}
          .ca-field{margin-bottom:14px;padding:0 14px;}
          .ca-label{display:block;font-weight:600;font-size:13px;color:#555;margin-bottom:6px;}
          .ca-required{color:#dc3545;}
          .ca-textarea{
            width:100%;padding:10px 12px;border:1.5px solid #f28b82;border-radius:6px;
            min-height:80px;resize:vertical;font-family:inherit;font-size:13px;
            background:#fff;box-sizing:border-box;transition:border-color 0.2s;
          }
          .ca-textarea:focus{border-color:#d32f2f;outline:none;box-shadow:0 0 0 3px rgba(211,47,47,0.12);}
          .ca-textarea.error,.ca-date-input.error,.ca-text-input.error{border-color:#dc3545;background:#fff5f5;}
          .ca-textarea.highlight{animation:caHighlight 1.8s forwards;}
          @keyframes caHighlight{0%{background:#dcfce7;border-color:#16a34a;}100%{background:#fff;border-color:#f28b82;}}

          /* ── Comment Section ── */
          .comment-section{margin-top:16px;}
          .comment-label{font-weight:600;color:#555;margin-bottom:8px;font-size:13px;}
          .comment-input{
            width:100%;padding:10px;border:1px solid #ddd;border-radius:4px;
            min-height:70px;resize:vertical;font-family:inherit;box-sizing:border-box;
          }

          /* ── Unified Card Action Bar ── */
          .kpi-card-actions {
           display: flex;
           align-items: center;
           justify-content: flex-start; /* ⬅️ instead of space-between */
           gap: 10px; /* ⬅️ space between buttons */
           margin-top: 16px;
           padding-top: 14px;
           border-top: 1px solid #e5e7eb;
           flex-wrap: nowrap; /* ⬅️ keep them on same row */
           }
          .kpi-card-actions-left{display:flex;align-items:center;gap:10px;flex-wrap:wrap;}
          .kpi-card-actions-right{display:flex;align-items:center;gap:10px;}

          .card-action-btn{
            display:inline-flex;align-items:center;gap:7px;
            border:none;border-radius:10px;
            padding:9px 16px;
            font-size:13px;font-weight:700;cursor:pointer;
            transition:transform 0.15s,box-shadow 0.15s,background 0.15s;
          }
          .card-action-btn svg{width:15px;height:15px;flex-shrink:0;}
          .card-action-btn:hover{transform:translateY(-1px);}

          .card-action-btn--ghost{
            background:#f8fafc;color:#475569;
            border:1px solid #d1d5db;
          }
          .card-action-btn--ghost:hover{background:#f1f5f9;box-shadow:0 4px 10px rgba(0,0,0,0.07);}

          .card-action-btn--primary{
            background:linear-gradient(135deg,#ef4444,#dc2626);
            color:white;
            box-shadow:0 6px 16px rgba(220,38,38,0.22);
          }
          .card-action-btn--primary:hover{box-shadow:0 8px 20px rgba(220,38,38,0.30);}

       .card-action-btn--ai{
  position:relative;
  overflow:hidden;
  background:linear-gradient(135deg,#2f87d6,#ff944d);
  color:white;
  box-shadow:0 6px 16px rgba(47,135,214,0.24);
  padding:9px 18px;
  isolation:isolate;
}

.card-action-btn--ai:hover{
  box-shadow:0 10px 22px rgba(47,135,214,0.34);
  transform:translateY(-1px) scale(1.02);
}

.ai-btn-glow{
  position:absolute;
  inset:0;
  background:radial-gradient(circle at 30% 50%,rgba(255,255,255,0.18),transparent 60%);
  pointer-events:none;
  animation:aiPulseGlow 2.8s ease-in-out infinite;
  z-index:0;
}

.ai-btn-shine{
  position:absolute;
  top:-20%;
  left:-35%;
  width:38%;
  height:140%;
  transform:rotate(18deg);
  background:linear-gradient(
    90deg,
    rgba(255,255,255,0) 0%,
    rgba(255,255,255,0.18) 50%,
    rgba(255,255,255,0) 100%
  );
  animation:aiShineSweep 3.2s linear infinite;
  pointer-events:none;
  z-index:0;
}

.ai-btn-icon,
.ai-btn-text{
  position:relative;
  z-index:1;
}

.ai-btn-icon{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  animation:aiRobotFloat 2.2s ease-in-out infinite;
}

@keyframes aiPulseGlow{
  0%,100%{opacity:0.65;}
  50%{opacity:1;}
}

@keyframes aiShineSweep{
  0%{left:-35%;}
  100%{left:120%;}
}

@keyframes aiRobotFloat{
  0%,100%{transform:translateY(0);}
  50%{transform:translateY(-2px);}
}

          /* ── CA Table Modal ── */
          .ca-modal-overlay{
            position:fixed;inset:0;
            background:rgba(15,23,42,0.60);
            display:flex;align-items:center;justify-content:center;
            padding:24px;z-index:10005;
            opacity:0;pointer-events:none;
            transition:opacity 0.22s ease;
          }
          .ca-modal-overlay.active{opacity:1;pointer-events:auto;}
          .ca-modal-box{
            width:min(98vw,980px);
            max-height:min(90vh,820px);
            background:#fff;border-radius:22px;
            box-shadow:0 30px 80px rgba(15,23,42,0.30);
            display:flex;flex-direction:column;overflow:hidden;
            transform:translateY(18px) scale(0.98);
            transition:transform 0.22s ease;
          }
          .ca-modal-overlay.active .ca-modal-box{transform:translateY(0) scale(1);}
          .ca-modal-header{
            display:flex;align-items:flex-start;justify-content:space-between;gap:16px;
            padding:22px 26px 18px;
            border-bottom:1px solid #fee2e2;
            background:linear-gradient(180deg,#fff5f5,#fff8f5);
          }
          .ca-modal-header-text{}
          .ca-modal-title{margin:0;font-size:20px;font-weight:900;color:#991b1b;}
          .ca-modal-subtitle{margin:5px 0 0;font-size:13px;color:#64748b;}
          .ca-modal-close{
            width:40px;height:40px;border:none;border-radius:999px;
            background:#fee2e2;color:#b91c1c;font-size:22px;line-height:1;
            cursor:pointer;flex-shrink:0;transition:background 0.2s;
          }
          .ca-modal-close:hover{background:#fecaca;}
          .ca-modal-body{flex:1;min-height:0;overflow:auto;padding:22px 26px;}

          /* Table */
          .ca-table-wrap{overflow-x:auto;border-radius:14px;border:1px solid #fee2e2;}
          .ca-table{
            width:100%;border-collapse:collapse;font-size:13px;
          }
          .ca-table thead tr{background:#fff5f5;}
          .ca-table th{
            padding:12px 14px;text-align:left;font-size:11px;font-weight:800;
            text-transform:uppercase;letter-spacing:0.6px;color:#991b1b;
            border-bottom:2px solid #fecaca;white-space:nowrap;
          }
          .ca-table td{
            padding:12px 14px;border-bottom:1px solid #fef2f2;
            vertical-align:top;color:#374151;line-height:1.5;
          }
          .ca-col-actions{
            width: 130px;
            text-align: right;
            white-space: nowrap;
          }

          .ca-table td.ca-col-actions{
            text-align: right;
            white-space: nowrap;
          }


.ca-table-edit-btn,
.ca-table-delete-btn{
  appearance: none;
  border: 1px solid transparent;
  border-radius: 10px;
  height: 34px;
  min-width: 34px;
  padding: 0 12px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  font-weight: 700;
  line-height: 1;
  cursor: pointer;
  transition:
    background 0.18s ease,
    color 0.18s ease,
    border-color 0.18s ease,
    box-shadow 0.18s ease,
    transform 0.18s ease;
}

.ca-table-edit-btn{
  background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
  color: #1d4ed8;
  border-color: #bfdbfe;
  box-shadow: 0 2px 6px rgba(29, 78, 216, 0.08);
}

.ca-table-edit-btn:hover{
  background: #eff6ff;
  border-color: #93c5fd;
  box-shadow: 0 6px 14px rgba(29, 78, 216, 0.14);
  transform: translateY(-1px);
}
  .ca-table-edit-btn:active{
  transform: translateY(0);
  box-shadow: 0 2px 6px rgba(29, 78, 216, 0.08);
}

.ca-table-delete-btn{
  background: linear-gradient(180deg, #ffffff 0%, #fff7f7 100%);
  color: #dc2626;
  border-color: #fecaca;
  box-shadow: 0 2px 6px rgba(220, 38, 38, 0.08);
  padding: 0;
  font-size: 15px;
  font-weight: 600;
}

.ca-table-delete-btn:hover{
  background: #fef2f2;
  border-color: #fca5a5;
  box-shadow: 0 6px 14px rgba(220, 38, 38, 0.14);
  transform: translateY(-1px);
}

.ca-table-delete-btn:active{
  transform: translateY(0);
  box-shadow: 0 2px 6px rgba(220, 38, 38, 0.08);
}

.ca-table-edit-btn:focus-visible,
.ca-table-delete-btn:focus-visible{
  outline: none;
  box-shadow:
    0 0 0 3px rgba(59, 130, 246, 0.18),
    0 4px 12px rgba(15, 23, 42, 0.10);
}

.ca-table-delete-btn:focus-visible{
  box-shadow:
    0 0 0 3px rgba(239, 68, 68, 0.18),
    0 4px 12px rgba(15, 23, 42, 0.10);
}

          .ca-table tr:last-child td{border-bottom:none;}
          .ca-table tr:hover td{background:#fff5f5;}
          .ca-table .ca-col-num{width:36px;font-weight:800;color:#b91c1c;}
          .ca-table .ca-col-actions{width:100px;text-align:right;white-space:nowrap;}
          .ca-table-empty{
            text-align:center;padding:40px 20px;color:#94a3b8;font-size:14px;
          }
          .ca-table-status{
            display:inline-block;padding:3px 10px;border-radius:999px;
            font-size:11px;font-weight:700;
            background:#f1f5f9;color:#475569;border:1px solid #e2e8f0;
          }
          .ca-table-status.open{background:#fef2f2;color:#b91c1c;border-color:#fecaca;}
          .ca-table-status.closed,.ca-table-status.completed{background:#ecfdf5;color:#047857;border-color:#a7f3d0;}
          .ca-table-status.waiting-for-validation{background:#fff7ed;color:#c2410c;border-color:#fed7aa;}

          .ca-tbl-btn{
            border:none;border-radius:7px;padding:5px 11px;font-size:11px;font-weight:700;cursor:pointer;
          }
          .ca-tbl-edit{background:#eff6ff;color:#1d4ed8;}
          .ca-tbl-edit:hover{background:#dbeafe;}
          .ca-tbl-delete{background:#fef2f2;color:#dc2626;margin-left:5px;}
          .ca-tbl-delete:hover{background:#fee2e2;}

          /* Modal form */
          .ca-modal-form-section{
            margin-top:22px;
            border:1.5px solid #fecaca;
            border-radius:16px;
            overflow:hidden;
          }
          .ca-modal-form-header{
            display:flex;align-items:center;justify-content:space-between;
            padding:14px 18px;background:#fff5f5;
            border-bottom:1px solid #fecaca;
          }
          .ca-modal-form-title{font-size:14px;font-weight:800;color:#991b1b;}
          .ca-modal-form-body{padding:18px;display:grid;gap:14px;}
          .ca-modal-form-row{display:grid;grid-template-columns:1fr 1fr;gap:14px;}
          .ca-modal-field label{display:block;font-size:12px;font-weight:700;color:#475569;margin-bottom:6px;text-transform:uppercase;letter-spacing:0.5px;}
          .ca-modal-input,.ca-modal-textarea,.ca-modal-select{
            width:100%;padding:10px 12px;border:1.5px solid #f28b82;border-radius:8px;
            font-size:13px;font-family:inherit;background:#fff;box-sizing:border-box;
          }
          .ca-modal-textarea{min-height:72px;resize:vertical;}
          .ca-modal-input:focus,.ca-modal-textarea:focus,.ca-modal-select:focus{
            border-color:#d32f2f;outline:none;box-shadow:0 0 0 3px rgba(211,47,47,0.10);
          }
          .ca-modal-form-footer{
            display:flex;justify-content:flex-end;gap:10px;
            padding:14px 18px;
            border-top:1px solid #fecaca;background:#fff5f5;
          }
          .ca-modal-cancel-btn{
            padding:9px 20px;border:1px solid #d1d5db;border-radius:8px;
            background:#fff;color:#374151;font-size:13px;font-weight:700;cursor:pointer;
          }
          .ca-modal-save-btn{
            padding:9px 24px;border:none;border-radius:8px;
            background:linear-gradient(135deg,#ef4444,#dc2626);
            color:white;font-size:13px;font-weight:700;cursor:pointer;
            box-shadow:0 4px 12px rgba(220,38,38,0.22);
          }
          .ca-modal-add-row-btn{
            display:inline-flex;align-items:center;gap:8px;
            margin-top:16px;
            padding:10px 18px;border:1.5px dashed #fca5a5;border-radius:10px;
            background:transparent;color:#b91c1c;font-size:13px;font-weight:700;cursor:pointer;
            transition:background 0.15s;width:100%;justify-content:center;
          }
          .ca-modal-add-row-btn:hover{background:#fff5f5;}

          /* ── Modals (chart + history) ── */
          body.chart-modal-open{overflow:hidden;}
          .chart-modal-overlay{
            position:fixed;inset:0;padding:24px;background:rgba(15,23,42,0.58);
            display:flex;align-items:center;justify-content:center;z-index:10003;
            opacity:0;pointer-events:none;transition:opacity 0.22s ease;
          }
          .chart-modal-overlay.active{opacity:1;pointer-events:auto;}
          .chart-modal-box{
            width:min(96vw,1700px);height:min(92vh,980px);
            background:#fff;border-radius:20px;
            box-shadow:0 28px 80px rgba(15,23,42,0.28);
            display:flex;flex-direction:column;overflow:hidden;
            transform:translateY(18px) scale(0.98);transition:transform 0.22s ease;
          }
          .chart-modal-overlay.active .chart-modal-box{transform:translateY(0) scale(1);}
          .chart-modal-header{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;padding:24px 28px 18px;border-bottom:1px solid #e5e7eb;}
          .chart-modal-header h3{margin:0;color:#0f172a;font-size:32px;line-height:1.15;}
          .chart-modal-header p{margin:8px 0 0;color:#64748b;font-size:15px;}
          .chart-modal-close{width:44px;height:44px;border:none;border-radius:999px;background:#f8fafc;color:#334155;font-size:28px;line-height:1;cursor:pointer;flex-shrink:0;}
          .chart-modal-close:hover{background:#e2e8f0;color:#0f172a;}
          .chart-modal-body{flex:1;min-height:0;padding:20px 24px 24px;background:linear-gradient(180deg,#ffffff 0%,#f8fafc 100%);}
          .chart-modal-stage{height:100%;min-height:0;background:#fff;border:1px solid #dbe4f0;border-radius:16px;padding:20px;box-shadow:inset 0 1px 0 rgba(255,255,255,0.8);}
          .chart-modal-stage canvas{width:100% !important;height:100% !important;display:block;}

        .history-modal-overlay{
           position:fixed;
           inset:0;
           padding:12px;
           background:rgba(15,23,42,0.58);
           display:flex;
           align-items:center;
           justify-content:center;
           z-index:10004;
           opacity:0;
           pointer-events:none;
          transition:opacity 0.22s ease;
          }
          .history-modal-overlay.active{opacity:1;pointer-events:auto;}
         .history-modal-box{
          width:min(98vw,1400px);
          height:min(94vh,1000px);
          max-height:min(94vh,1000px);
          background:#fff;
         border-radius:22px;
         overflow:hidden;
         box-shadow:0 28px 80px rgba(15,23,42,0.28);
         transform:translateY(18px) scale(0.98);
         transition:transform 0.22s ease;
         display:flex;
         flex-direction:column;
         }
          .history-modal-overlay.active .history-modal-box{transform:translateY(0) scale(1);}
          .history-modal-header{
           display:flex;
           align-items:flex-start;
           justify-content:space-between;
           gap:16px;
           padding:24px 28px 18px;
           border-bottom:1px solid #e5e7eb;
           background:linear-gradient(180deg,#ffffff,#f8fbff);
           flex-shrink:0;
             }
          .history-modal-title{margin:0;color:#0f172a;font-size:24px;line-height:1.2;}
          .history-modal-subtitle{margin:8px 0 0;color:#64748b;font-size:14px;}
          .history-modal-close{width:42px;height:42px;border:none;border-radius:999px;background:#f8fafc;color:#334155;font-size:26px;line-height:1;cursor:pointer;flex-shrink:0;}
          .history-modal-content{
           flex:1;
           min-height:0;
           padding:24px 28px 28px;
           overflow:auto;
           background:linear-gradient(180deg,#ffffff,#f8fafc 100%);
          }
          .history-meta-row{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:18px;}
          .history-chip{display:inline-flex;align-items:center;gap:8px;padding:9px 12px;border-radius:999px;font-size:12px;font-weight:800;background:#eff6ff;color:#1d4ed8;}
          .history-chip.status-open{background:#fef2f2;color:#b91c1c;}
          .history-chip.status-waiting-for-validation{background:#fff7ed;color:#c2410c;}
          .history-chip.status-completed,.history-chip.status-closed{background:#ecfdf5;color:#047857;}
          .history-section{background:white;border:1px solid #e5e7eb;border-radius:18px;padding:18px;box-shadow:0 8px 22px rgba(15,23,42,0.05);}
          .history-section+.history-section{margin-top:16px;}
          .history-section-title{margin:0 0 14px;font-size:14px;font-weight:900;color:#111827;display:flex;align-items:center;gap:8px;}
          .history-detail-card{background:linear-gradient(135deg,#fff8f8,#fffaf5);border:1px solid #fee2e2;border-radius:16px;padding:16px;}
          .history-detail-row+.history-detail-row{margin-top:14px;padding-top:14px;border-top:1px solid rgba(226,232,240,0.8);}
          .history-detail-label{font-size:12px;font-weight:900;margin-bottom:6px;}
          .history-detail-label.root{color:#dc2626;}
          .history-detail-label.solution{color:#d97706;}
          .history-detail-label.evidence{color:#2563eb;}
          .history-detail-text{font-size:14px;line-height:1.6;color:#334155;white-space:pre-wrap;}
          .history-comments-list{display:grid;gap:12px;}
          .history-comment-card{border-radius:16px;padding:16px;background:linear-gradient(135deg,#eff6ff,#dbeafe);border-left:4px solid #0f6cbd;}
          .history-comment-label{font-size:12px;font-weight:900;color:#2563eb;margin-bottom:8px;}
          .history-comment-text{font-size:14px;color:#1e293b;line-height:1.6;white-space:pre-wrap;}
          .history-empty{text-align:center;padding:34px 20px;background:white;border:1px dashed #cbd5e1;border-radius:18px;color:#64748b;font-size:14px;line-height:1.6;}

          /* ── Global Loading / Submit Modal ── */
          .loading-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:9999;flex-direction:column;align-items:center;justify-content:center;gap:20px;}
          .loading-overlay.active{display:flex;}
          .spinner{width:56px;height:56px;border:6px solid rgba(255,255,255,0.3);border-top-color:#fff;border-radius:50%;animation:spin 0.9s linear infinite;}
          @keyframes spin{to{transform:rotate(360deg)}}
          .loading-text{color:#fff;font-size:18px;font-weight:600;}
          .modal-overlay{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.6);display:flex;align-items:center;justify-content:center;z-index:10000;opacity:0;pointer-events:none;transition:all 0.25s ease;}
          .modal-overlay.active{opacity:1;pointer-events:all;}
          .modal-box{background:white;border-radius:12px;padding:30px 25px;width:90%;max-width:420px;text-align:center;box-shadow:0 10px 40px rgba(0,0,0,0.25);transform:translateY(20px);transition:transform 0.25s ease;}
          .modal-overlay.active .modal-box{transform:translateY(0);}
          .modal-icon{font-size:42px;margin-bottom:10px;}
          .modal-box h3{margin:10px 0;font-size:20px;color:#333;}
          .modal-box p{color:#666;font-size:14px;margin-bottom:25px;}
          .modal-actions{display:flex;gap:12px;justify-content:center;}
          .btn-cancel{padding:10px 20px;border:1px solid #ccc;background:white;border-radius:6px;cursor:pointer;font-weight:600;}
          .btn-confirm{padding:10px 20px;border:none;background:linear-gradient(135deg,#0078D7,#005ea6);color:white;border-radius:6px;cursor:pointer;font-weight:600;}
          .submit-btn{background:#0078D7;color:white;border:none;padding:12px 30px;border-radius:4px;font-size:16px;font-weight:600;cursor:pointer;display:block;width:100%;margin-top:20px;}

          /* ── AI Assistant ── */
          .assistant-shell{position:fixed;right:90px;bottom:24px;z-index:10001;}
          .assistant-launcher{
           width:64px;
           height:64px;
           border:none;
           border-radius:999px;
           background:linear-gradient(135deg,#2f87d6,#ff944d);
           color:white;
           box-shadow:0 18px 35px rgba(15,23,42,0.26);
           font-size:30px;
           cursor:pointer;
           animation:assistantLauncherPulse 2.4s ease-in-out infinite;
           transition:transform 0.18s ease, box-shadow 0.18s ease;
          }
          .assistant-launcher:hover{
           transform:translateY(-6px) scale(1.04);
           box-shadow:0 22px 42px rgba(15,23,42,0.32);
          }

          @keyframes assistantLauncherPulse{
          0%,100%{
           box-shadow:0 18px 35px rgba(15,23,42,0.26), 0 0 0 0 rgba(47,135,214,0.28);
            }
          50%{
            box-shadow:0 18px 35px rgba(15,23,42,0.26), 0 0 0 10px rgba(47,135,214,0);
           }
          }

         .assistant-cursor{
           display:inline-block;
            margin-left:2px;
            color:#0f6cbd;
            animation:assistantBlink 0.8s step-end infinite;
           }

           @keyframes assistantBlink{
           50%{opacity:0;}
          }
          .assistant-panel{
           width:min(92vw,460px);
           height:min(72vh,620px);
           background:rgba(255,255,255,0.98);
           border:1px solid #dbe7f4;
           border-radius:26px;
           box-shadow:0 30px 60px rgba(15,23,42,0.22);
           overflow:hidden;
           display:flex;
           flex-direction:column;
           margin-bottom:18px;
           backdrop-filter:blur(12px);
           opacity:0;
           transform:translateY(18px) scale(0.96);
           pointer-events:none;
           visibility:hidden;
           transition:
           opacity 0.28s ease,
           transform 0.28s ease,
           visibility 0.28s ease;
         }

          .assistant-shell.open .assistant-panel{
            opacity:1;
            transform:translateY(0) scale(1);
            pointer-events:auto;
            visibility:visible;
              }
          .assistant-header{display:flex;align-items:center;justify-content:space-between;padding:18px 20px;border-bottom:1px solid #e5edf6;background:linear-gradient(180deg,#ffffff,#f6fbff);}
          .assistant-title-wrap{display:flex;align-items:center;gap:12px;}
          .assistant-avatar{width:42px;height:42px;border-radius:999px;display:flex;align-items:center;justify-content:center;background:#eef6ff;color:#0f6cbd;font-size:22px;border:1px solid #c6def5;}
          .assistant-title{font-size:16px;font-weight:800;color:#0f6cbd;}
          .assistant-focus{margin-top:4px;font-size:12px;color:#5b6b7b;}
          .assistant-close{border:none;background:transparent;color:#5b6b7b;font-size:24px;cursor:pointer;line-height:1;}
          .assistant-messages{flex:1;padding:18px;overflow:auto;background:linear-gradient(180deg,#ffffff,#f8fbff 55%,#ffffff);display:flex;flex-direction:column;gap:14px;}
          .assistant-message{max-width:85%;padding:14px 16px;border-radius:20px;line-height:1.55;font-size:14px;white-space:pre-wrap;}
          .assistant-message.assistant{align-self:flex-start;background:#eef4fb;color:#1f2a37;border:1px solid #d7e2ee;}
          .assistant-message.user{align-self:flex-end;background:#0f6cbd;color:white;}
          .assistant-composer{padding:16px;border-top:1px solid #e5edf6;background:#fbfdff;}
          .assistant-form{display:flex;align-items:flex-end;gap:12px;padding:8px;border:2px solid #8cc4f3;border-radius:18px;background:white;box-shadow:0 8px 18px rgba(15,108,189,0.08);}
          .assistant-input{flex:1;border:none;resize:none;min-height:48px;max-height:140px;font:inherit;padding:10px 12px;background:transparent;}
          .assistant-input:focus{outline:none;}
          .assistant-send{width:48px;height:48px;border:none;border-radius:14px;background:linear-gradient(135deg,#5aa7e8,#79b8ee);color:white;font-size:22px;cursor:pointer;flex-shrink:0;}
          .assistant-status{margin-bottom:10px;font-size:12px;color:#6a7a8a;}

          /* ── Responsive ── */
          @media(max-width:900px){
            .kpi-split-layout{grid-template-columns:1fr;}
            .chart-modal-overlay{padding:14px;}
            .chart-modal-box{width:100%;height:min(90vh,900px);}
            .chart-modal-header{padding:18px 18px 14px;}
            .chart-modal-header h3{font-size:24px;}
            .chart-modal-body{padding:14px;}
            .history-modal-overlay{padding:8px;}
            .history-modal-box{
              width:100%;
              height:94vh;
              max-height:94vh;
              border-radius:16px;
             }
            .ca-modal-box{max-height:min(94vh,820px);}
          }
          @media(max-width:700px){
            .ca-dates-grid{grid-template-columns:1fr;}
            .ca-modal-form-row{grid-template-columns:1fr;}
            .kpi-card-actions{flex-direction:column;align-items:stretch;}
            .kpi-card-actions-right{justify-content:flex-end;}
          }
          @media(max-width:600px){
            .ca-ai-row{grid-template-columns:1fr;}
            .assistant-shell{right:16px;bottom:16px;}
            .assistant-panel{height:min(76vh,620px);}
          }
        </style>
      </head>
      <body>
        <!-- Loading overlay -->
        <div class="loading-overlay" id="loadingOverlay">
          <div class="spinner"></div>
          <div class="loading-text">Submitting KPI Values...</div>
        </div>

        <!-- Submit confirm modal -->
        <div id="confirmModal" class="modal-overlay">
          <div class="modal-box">
            <div class="modal-icon">⚠️</div>
            <h3>Confirm Submission</h3>
            <p>Are you sure you want to submit your KPI values?</p>
            <div class="modal-actions">
              <button id="cancelBtn" type="button" class="btn-cancel">Cancel</button>
              <button id="confirmBtn" type="button" class="btn-confirm">Yes, Submit</button>
            </div>
          </div>
        </div>

        <!-- Chart expand modal -->
        <div id="chartModal" class="chart-modal-overlay" aria-hidden="true">
          <div class="chart-modal-box" role="dialog" aria-modal="true" aria-labelledby="chartModalTitle">
            <div class="chart-modal-header">
              <div>
                <h3 id="chartModalTitle">KPI Trend</h3>
                <p id="chartModalSubtitle">Monthly KPI performance overview</p>
              </div>
              <button id="chartModalClose" type="button" class="chart-modal-close" aria-label="Close">&times;</button>
            </div>
            <div class="chart-modal-body">
              <div class="chart-modal-stage">
                <canvas id="chartModalCanvas"></canvas>
              </div>
            </div>
          </div>
        </div>

        <!-- History modal -->
        <div id="historyModal" class="history-modal-overlay" aria-hidden="true">
          <div class="history-modal-box" role="dialog" aria-modal="true" aria-labelledby="historyModalTitle">
            <div class="history-modal-header">
              <div>
                <h3 id="historyModalTitle" class="history-modal-title">Corrective Action History</h3>
                <p id="historyModalSubtitle" class="history-modal-subtitle">Previous month details for this KPI</p>
              </div>
              <button id="historyModalClose" type="button" class="history-modal-close" aria-label="Close">&times;</button>
            </div>
            <div id="historyModalContent" class="history-modal-content"></div>
          </div>
        </div>

        <!-- ── CA Table Modal ── -->
        <div id="caTableModal" class="ca-modal-overlay" aria-hidden="true">
          <div class="ca-modal-box" role="dialog" aria-modal="true" aria-labelledby="caModalTitle">
            <div class="ca-modal-header">
              <div class="ca-modal-header-text">
                <h3 id="caModalTitle" class="ca-modal-title">Corrective Actions</h3>
                <p id="caModalSubtitle" class="ca-modal-subtitle">Manage corrective action entries for this KPI</p>
              </div>
              <button id="caModalClose" type="button" class="ca-modal-close" aria-label="Close">&times;</button>
            </div>
            <div class="ca-modal-body">
              <!-- Table of existing actions -->
              <div class="ca-table-wrap">
                <table class="ca-table" id="caModalTable">
                  <thead>
                    <tr>
                      <th class="ca-col-num">#</th>
                      <th>Root Cause</th>
                      <th>Immediate Action</th>
                      <th>Evidence</th>
                      <th>Due Date</th>
                      <th>Responsible</th>
                      <th>Status</th>
                      <th class="ca-col-actions">Actions</th>
                    </tr>
                  </thead>
                  <tbody id="caModalTableBody">
                    <tr><td colspan="8" class="ca-table-empty">No corrective actions yet.</td></tr>
                  </tbody>
                </table>
              </div>

              <!-- Add new / Edit form -->
              <button type="button" class="ca-modal-add-row-btn" id="caModalAddRowBtn">
                <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M7 1v12M1 7h12"/></svg>
                Add New Corrective Action
              </button>

              <div class="ca-modal-form-section" id="caModalFormSection" style="display:none;">
                <div class="ca-modal-form-header">
                  <span class="ca-modal-form-title" id="caModalFormTitle">New Corrective Action</span>
                  <button type="button" class="ca-tbl-btn ca-tbl-delete" id="caModalFormCollapse">✕ Cancel</button>
                </div>
                <div class="ca-modal-form-body">
                  <input type="hidden" id="caModalEditIndex" value="">
                  <div class="ca-modal-form-row">
                    <div class="ca-modal-field">
                      <label>Due Date <span class="ca-required">*</span></label>
                      <input type="date" id="caModalDueDate" class="ca-modal-input">
                    </div>
                    <div class="ca-modal-field">
                      <label>Responsible <span class="ca-required">*</span></label>
                      <input type="text" id="caModalResponsible" class="ca-modal-input" placeholder="Name">
                    </div>
                  </div>
                  <div class="ca-modal-field">
                    <label>Root Cause <span class="ca-required">*</span></label>
                    <textarea id="caModalRootCause" class="ca-modal-textarea" placeholder="Describe the root cause..."></textarea>
                  </div>
                  <div class="ca-modal-field">
                    <label>Immediate Action / Solution <span class="ca-required">*</span></label>
                    <textarea id="caModalSolution" class="ca-modal-textarea" placeholder="Describe the implemented solution..."></textarea>
                  </div>
                </div>
                <div class="ca-modal-form-footer">
                  <button type="button" class="ca-modal-cancel-btn" id="caModalCancelForm">Cancel</button>
                  <button type="button" class="ca-modal-save-btn" id="caModalSaveForm">Save Action</button>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Main container -->
        <div class="container">
          <div class="header">
            <h2 style="color:white;font-size:22px;margin:0;">KPI Submission - ${week}</h2>
          </div>

          <div class="form-section">
            <div class="info-section">
              <div class="info-row"><div class="info-label">Name</div><div class="info-value">${responsible.name}</div></div>
              <div class="info-row"><div class="info-label">Group</div><div class="info-value">${responsible.plant_name}</div></div>
              <div class="info-row"><div class="info-label">Department</div><div class="info-value">${responsible.department_name}</div></div>
              <div class="info-row"><div class="info-label">Week</div><div class="info-value">${week}</div></div>
            </div>

            <div class="kpi-section">
              <h3 style="color:#0078D7;margin-bottom:20px;border-bottom:2px solid #0078D7;padding-bottom:8px;">KPI Values</h3>
              <form action="/redirect" method="POST" id="kpiForm" novalidate>
                <input type="hidden" name="responsible_id" value="${responsible_id}" />
                <input type="hidden" name="week" value="${week}" />
                ${kpiCardsHtml}
                <button type="submit" class="submit-btn">Submit KPI Values</button>
              </form>
            </div>
          </div>
        </div>

        <!-- AI Assistant -->
        <div class="assistant-shell" id="assistantShell">
          <div class="assistant-panel" id="assistantPanel">
            <div class="assistant-header">
              <div class="assistant-title-wrap">
                <div class="assistant-avatar">🤖</div>
                <div>
                  <div class="assistant-title">AI Assistant</div>
                  <div class="assistant-focus" id="assistantFocus">All KPIs on this form</div>
                </div>
              </div>
              <button type="button" class="assistant-close" id="assistantClose">×</button>
            </div>
            <div class="assistant-messages" id="assistantMessages"></div>
            <div class="assistant-composer">
              <div class="assistant-status" id="assistantStatus">Ask about KPI trends, quotation delays, root causes, owners, or corrective actions.</div>
              <form class="assistant-form" id="assistantForm">
                <textarea class="assistant-input" id="assistantInput" placeholder="Ask about KPIs or quote delays" rows="1"></textarea>
                <button type="submit" class="assistant-send" id="assistantSend">➤</button>
              </form>
            </div>
          </div>
      
        </div>

        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script>
          /* ═══════════════════════════════════════════════
             CA TABLE MODAL
          ═══════════════════════════════════════════════ */
          let caModalKvId = null;
          // In-memory store: kvId → array of action objects
          const caModalStore = {};

function getCaModalActions(kvId) {
  if (!caModalStore[kvId]) {
    caModalStore[kvId] = [];

    getCorrectiveActionCards(kvId).forEach((card) => {
      const rootCause = getTrimmedValue(getCorrectiveActionField(card, "root_cause"));
      const implSolution = getTrimmedValue(getCorrectiveActionField(card, "implemented_solution"));
      const dueDate = getInputValue(getCorrectiveActionField(card, "due_date"));
      const responsible = getTrimmedValue(getCorrectiveActionField(card, "responsible"));
      const actionId = getTrimmedValue(
        card.querySelector('input[name="ca_action_id_' + kvId + '[]"]')
      );

      if (rootCause || implSolution || dueDate || responsible || actionId) {
        caModalStore[kvId].push({
        id: actionId,
        root_cause: rootCause,
       implemented_solution: implSolution, // ✅ FIXED
       due_date: dueDate,
       responsible: responsible,
       status: "Open"
      });
      }
    });
  }

  return caModalStore[kvId];
}

          function escapeHtml(v) {
            return String(v || "")
              .replace(/&/g, "&amp;").replace(/</g, "&lt;")
              .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
          }

          function statusClass(s) {
            return String(s || "").trim().toLowerCase().replace(/\s+/g, "-");
          }

          function truncate(str, n) {
            const s = String(str || "");
            return s.length > n ? s.slice(0, n) + "…" : s;
          }

        function renderCaModalTable(kvId) {
       const tbody = document.getElementById("caModalTableBody");
       if (!tbody) return;
       const actions = getCaModalActions(kvId);
       if (!actions.length) {
       tbody.innerHTML = '<tr><td colspan="8" class="ca-table-empty">No corrective actions yet. Click "Add" below to get started.</td></tr>';
       return;
       }

       tbody.innerHTML = actions.map((a, i) => {
      const sc = statusClass(a.status);
      return \`<tr>
      <td class="ca-col-num">\${i + 1}</td>
      <td title="\${escapeHtml(a.root_cause)}">\${escapeHtml(truncate(a.root_cause, 60))}</td>
      <td title="\${escapeHtml(a.implemented_solution)}">\${escapeHtml(truncate(a.implemented_solution, 60))}</td>
      <td title="\${escapeHtml(a.evidence || "")}">\${escapeHtml(truncate(a.evidence || "", 40))}</td>
      <td>\${escapeHtml(a.due_date)}</td>
      <td>\${escapeHtml(a.responsible)}</td>
      <td>\${a.status ? \`<span class="ca-table-status \${sc}">\${escapeHtml(a.status)}</span>\` : "—"}</td>
      <td class="ca-col-actions">
        <button type="button" class="ca-table-edit-btn" onclick="caModalOpenForm(\${i})">Edit</button>
        <button type="button" class="ca-table-delete-btn" onclick="caModalDeleteAction(\${i})">✕</button>
      </td>
       </tr>\`;
        }).join("");
      }

    function caModalOpenForm(editIndex) {
     const section = document.getElementById("caModalFormSection");
     const formTitle = document.getElementById("caModalFormTitle");
     const editIdx = document.getElementById("caModalEditIndex");
     if (!section) return;

     section.style.display = "";
     if (editIndex !== null && editIndex !== undefined && editIndex >= 0) {
     const actions = getCaModalActions(caModalKvId);
     const a = actions[editIndex];
     if (!a) return;
    if (formTitle) formTitle.textContent = "Edit Action #" + (editIndex + 1);
    if (editIdx) editIdx.value = String(editIndex);
    document.getElementById("caModalDueDate").value = a.due_date || "";
    document.getElementById("caModalResponsible").value = a.responsible || "";
    document.getElementById("caModalRootCause").value = a.root_cause || "";
    document.getElementById("caModalSolution").value = a.implemented_solution || "";
  } else {
    if (formTitle) formTitle.textContent = "New Corrective Action";
    if (editIdx) editIdx.value = "";
    ["caModalDueDate","caModalResponsible","caModalRootCause","caModalSolution"].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.value = "";
    });
  }

  section.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

          function caModalOpenEdit(index) {
            caModalOpenForm(index);
          }

          function caModalCollapseForm() {
            const section = document.getElementById("caModalFormSection");
            if (section) section.style.display = "none";
          }

function caModalSaveForm() {
  if (!caModalKvId) return;

  const rootCauseEl = document.getElementById("caModalRootCause");
  const solutionEl = document.getElementById("caModalSolution");
  const dueDateEl = document.getElementById("caModalDueDate");
  const responsibleEl = document.getElementById("caModalResponsible");
  const editIndexEl = document.getElementById("caModalEditIndex");

  const rootCause = rootCauseEl ? rootCauseEl.value.trim() : "";
  const solution = solutionEl ? solutionEl.value.trim() : "";
  const dueDate = dueDateEl ? dueDateEl.value : "";
  const responsible = responsibleEl ? responsibleEl.value.trim() : "";
  const editIndex = editIndexEl ? editIndexEl.value : "";

  if (!rootCause || !solution || !dueDate || !responsible) {
    alert("Please fill in Root Cause, Corrective Action, Due Date, and Responsible.");
    return;
  }

  const actions = getCaModalActions(caModalKvId);

  const entry = {
   id: "",
   root_cause: rootCause,
   implemented_solution: solution, // ✅ FIXED
   due_date: dueDate,
   responsible: responsible,
   status: "Open"
  };

  if (editIndex !== "" && !isNaN(parseInt(editIndex, 10))) {
    const idx = parseInt(editIndex, 10);
    if (actions[idx] && actions[idx].id) {
      entry.id = actions[idx].id;
    }
    actions[idx] = entry;
  } else {
    actions.push(entry);
  }

  renderCaModalTable(caModalKvId);
  syncDomFromStore(caModalKvId);
  caModalCollapseForm();
}

function caModalDeleteAction(index) {
  if (!caModalKvId) return;

  const actions = getCaModalActions(caModalKvId);
  actions.splice(index, 1);

  renderCaModalTable(caModalKvId);
  syncDomFromStore(caModalKvId);
}


function syncDomFromStore(kvId) {
  const stack = getCorrectiveActionStack(kvId);
  if (!stack) return;

  const actions = getCaModalActions(kvId);

  const existingCards = Array.from(stack.querySelectorAll(".ca-action-card"));
  const template = existingCards[0];
  if (!template) return;

  stack.innerHTML = "";

  actions.forEach((action, idx) => {
    const newCard = template.cloneNode(true);

    const idInput = newCard.querySelector('input[name="ca_action_id_' + kvId + '[]"]');
    if (idInput) idInput.value = action.id || "";

    const statusInput = newCard.querySelector('input[name="ca_status_' + kvId + '[]"]');
    if (statusInput) statusInput.value = "Open";

    const dueDateField = getCorrectiveActionField(newCard, "due_date");
    const responsibleField = getCorrectiveActionField(newCard, "responsible");
    const rootCauseField = getCorrectiveActionField(newCard, "root_cause");
    const implSolutionField = getCorrectiveActionField(newCard, "implemented_solution");

    if (dueDateField) dueDateField.value = action.due_date || "";
    if (responsibleField) responsibleField.value = action.responsible || "";
    if (rootCauseField) rootCauseField.value = action.root_cause || "";
    if (implSolutionField) implSolutionField.value = action.implemented_solution || "";

    const badge = newCard.querySelector(".ca-status-badge");
    if (badge) {
      badge.textContent = "Open";
      badge.className = "ca-status-badge ca-status-open";
    }

    const removeBtn = newCard.querySelector(".ca-remove-btn");
    if (removeBtn) {
      removeBtn.classList.toggle("is-hidden", actions.length === 1);
    }

    stack.appendChild(newCard);
    bindCorrectiveActionCard(newCard, kvId);
  });

  renumberCorrectiveActionCards(kvId);

  const countBadge = document.getElementById("ca-count-badge-" + kvId);
  if (countBadge) {
    countBadge.textContent = actions.length + " Action" + (actions.length === 1 ? "" : "s");
  }
}

          function openCaTableModal(kvId) {
            caModalKvId = kvId;
            const overlay = document.getElementById("caTableModal");
            const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');
            const title = card ? (card.querySelector(".kpi-title") || {}).textContent || "KPI" : "KPI";

            const modalTitle = document.getElementById("caModalTitle");
            const modalSubtitle = document.getElementById("caModalSubtitle");
            if (modalTitle) modalTitle.textContent = "Corrective Actions — " + title.trim();
            if (modalSubtitle) modalSubtitle.textContent = "Add, edit, or remove corrective action entries";

            caModalCollapseForm();
            renderCaModalTable(kvId);

            overlay.classList.add("active");
            overlay.setAttribute("aria-hidden", "false");
            document.body.classList.add("chart-modal-open");
          }

          function closeCaTableModal() {
            const overlay = document.getElementById("caTableModal");
            overlay.classList.remove("active");
            overlay.setAttribute("aria-hidden", "true");
            if (!document.querySelector(".chart-modal-overlay.active") &&
                !document.querySelector(".history-modal-overlay.active")) {
              document.body.classList.remove("chart-modal-open");
            }
            caModalKvId = null;
          }

          /* ═══════════════════════════════════════════════
             UTILITY HELPERS
          ═══════════════════════════════════════════════ */
          function getTextContent(node) { return node && typeof node.textContent === "string" ? node.textContent : ""; }
          function getTrimmedText(node) { return getTextContent(node).trim(); }
          function getInputValue(node) { return node && typeof node.value === "string" ? node.value : ""; }
          function getTrimmedValue(node) { return getInputValue(node).trim(); }

          function getCorrectiveActionStack(kvId) {
            return document.querySelector('.ca-actions-stack[data-kpi-values-id="' + kvId + '"]');
          }
          function getCorrectiveActionCards(kvId) {
            const stack = getCorrectiveActionStack(kvId);
            return stack ? Array.from(stack.querySelectorAll(".ca-action-card")) : [];
          }
          function getLatestCorrectiveActionCard(kvId) {
            const cards = getCorrectiveActionCards(kvId);
            return cards.length ? cards[cards.length - 1] : null;
          }
          function getCorrectiveActionField(actionCard, fieldName) {
            return actionCard ? actionCard.querySelector('[data-ca-field="' + fieldName + '"]') : null;
          }
          function renumberCorrectiveActionCards(kvId) {
            getCorrectiveActionCards(kvId).forEach((card, index) => {
              const num = card.querySelector(".ca-action-number");
              if (num) num.textContent = String(index + 1);
            });
            const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');
            const badge = card ? card.querySelector(".ca-count-badge") : null;
            const count = getCorrectiveActionCards(kvId).length;
            if (badge) badge.textContent = count + " Action" + (count === 1 ? "" : "s");
          }
          function bindCorrectiveActionCard(actionCard, kvId) {
            if (!actionCard) return;
            const removeButton = actionCard.querySelector(".ca-remove-btn");
            if (removeButton) {
              removeButton.onclick = () => {
                if (getCorrectiveActionCards(kvId).length <= 1) return;
                actionCard.remove();
                renumberCorrectiveActionCards(kvId);
              };
            }
          }

          function formApplyField(kvId, fieldName, card) {
            const text = getTrimmedText(card.querySelector(".ca-ai-card-text"));
            const actionCard = getLatestCorrectiveActionCard(kvId);
            const field = getCorrectiveActionField(actionCard, fieldName);
            if (!field || !text) return;
            field.value = text;
            field.classList.remove("highlight");
            void field.offsetWidth;
            field.classList.add("highlight");
            field.scrollIntoView({ behavior: "smooth", block: "center" });
            card.classList.add("applied");
            const hint = card.querySelector(".ca-apply-hint");
            if (hint) hint.textContent = "Applied";
          }
          window.formApplyField = formApplyField;

          function checkLowLimit(input) {
            const card = input.closest(".kpi-card");
            if (!card) return;
            const rawLow = parseFloat(card.dataset.lowLimit);
            const rawHigh = parseFloat(card.dataset.highLimit);
            const hasLow = !isNaN(rawLow), hasHigh = !isNaN(rawHigh);
            const lowerBound = hasLow && hasHigh ? Math.min(rawLow, rawHigh) : hasLow ? rawLow : hasHigh ? rawHigh : null;
            const upperBound = hasLow && hasHigh ? Math.max(rawLow, rawHigh) : hasHigh ? rawHigh : hasLow ? rawLow : null;
            const goodDirection = String(card.dataset.goodDirection || "up").toLowerCase() === "down" ? "down" : "up";
            const val = parseFloat(input.value);
            const kvId = input.dataset.kpiValuesId;
            const caPanel = document.getElementById("ca_container_" + kvId);
            const isOutside = !isNaN(val) && (
              goodDirection === "down"
                ? upperBound !== null && val > upperBound
                : lowerBound !== null && val < lowerBound
            );
            if (!caPanel) return;
            caPanel.classList.toggle("visible", isOutside);
            caPanel.querySelectorAll(".ca-required-field").forEach(f => { f.required = isOutside; });
          }

          function collectAssistantKpis() {
            return Array.from(document.querySelectorAll(".kpi-card")).map(card => {
              const kvId = card.dataset.kpiValuesId;
              const valueInput = card.querySelector(".value-input");
              const comment = getTrimmedValue(card.querySelector('textarea[name^="comment_"]'));
              const statusBadge = card.querySelector(".ca-status-badge");
              const actionCards = getCorrectiveActionCards(kvId);
              const correctiveActions = actionCards.map(ac => ({
                corrective_action_id: getTrimmedValue(ac.querySelector('input[name="ca_action_id_' + kvId + '[]"]')),
                root_cause: getTrimmedValue(getCorrectiveActionField(ac, "root_cause")),
                implemented_solution: getTrimmedValue(getCorrectiveActionField(ac, "implemented_solution")),
                evidence: getTrimmedValue(getCorrectiveActionField(ac, "evidence")),
                due_date: getInputValue(getCorrectiveActionField(ac, "due_date")),
                responsible: getTrimmedValue(getCorrectiveActionField(ac, "responsible"))
              }));
              const latestAction = correctiveActions.length ? correctiveActions[correctiveActions.length - 1] : {};
              return {
                kpi_id: card.dataset.kpiId,
                kpi_values_id: kvId,
                title: getTrimmedText(card.querySelector(".kpi-title")),
                subtitle: getTrimmedText(card.querySelector(".kpi-subtitle")),
                good_direction: card.dataset.goodDirection || "",
                current_value: getInputValue(valueInput),
                target: card.dataset.target || "",
                low_limit: card.dataset.lowLimit || "",
                high_limit: card.dataset.highLimit || "",
                unit: card.dataset.unit || "",
                week: card.dataset.currentWeek || "",
                comment,
                root_cause: latestAction.root_cause || "",
                implemented_solution: latestAction.implemented_solution || "",
                evidence: latestAction.evidence || "",
                due_date: latestAction.due_date || "",
                responsible: latestAction.responsible || "",
                corrective_action_status: getTrimmedText(statusBadge),
                corrective_actions: correctiveActions
              };
            });
          }

          /* ═══════════════════════════════════════════════
             AI ASSISTANT
          ═══════════════════════════════════════════════ */
          const assistantShell = document.getElementById("assistantShell");
          const assistantLauncher = document.getElementById("assistantLauncher");
          const assistantClose = document.getElementById("assistantClose");
          const assistantMessages = document.getElementById("assistantMessages");
          const assistantFocus = document.getElementById("assistantFocus");
          const assistantStatus = document.getElementById("assistantStatus");
          const assistantForm = document.getElementById("assistantForm");
          const assistantInput = document.getElementById("assistantInput");
          const assistantSend = document.getElementById("assistantSend");
          const assistantState = { selectedKpiId: null, booted: false, pending: false, greetingKey: null };

         function addAssistantMessage(role, text) {
  if (!assistantMessages) return;

  const msg = document.createElement("div");
  msg.className = "assistant-message " + role;
  assistantMessages.appendChild(msg);
  assistantMessages.scrollTop = assistantMessages.scrollHeight;

  const safeText = String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

  if (role !== "assistant") {
    msg.innerHTML = safeText.split("\\n").join("<br>");
    assistantMessages.scrollTop = assistantMessages.scrollHeight;
    return;
  }

  let i = 0;
  const typingSpeed = 16;

  function typeNext() {
    const partial = safeText.slice(0, i).split("\\n").join("<br>");
    msg.innerHTML = partial + '<span class="assistant-cursor">|</span>';
    assistantMessages.scrollTop = assistantMessages.scrollHeight;

    if (i < safeText.length) {
      i++;
      setTimeout(typeNext, typingSpeed);
    } else {
      msg.innerHTML = safeText.split("\\n").join("<br>");
    }
  }

  typeNext();
}
          function setAssistantStatus(text) { if (assistantStatus) assistantStatus.textContent = text; }
          function getKpiCardById(kvId) { return document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]'); }
          function getAssistantKpiDisplayName(card) {
            if (!card) return "Selected KPI";
            const title = getTrimmedText(card.querySelector(".kpi-title"));
            const subtitle = getTrimmedText(card.querySelector(".kpi-subtitle"));
            return subtitle && title ? (subtitle + " (" + title + ")") : (subtitle || title || "Selected KPI");
          }
          function resetAssistantConversation() {
            if (assistantMessages) assistantMessages.innerHTML = "";
            assistantState.booted = false;
            assistantState.greetingKey = null;
          }
          function buildAssistantGreeting() {
            if (!assistantState.selectedKpiId) {
              return "Hello! I can help with KPI context and with quotation or costing delay diagnosis based on the knowledge base. Ask about causes, actions, owners, metrics, or linked issues.";
            }
            const card = getKpiCardById(assistantState.selectedKpiId);
            const kpiName = getAssistantKpiDisplayName(card);
            return "Hello! I am focused on " + kpiName + ". I will analyze this KPI first and connect it to the estimating-delay knowledge base when relevant. Ask why it is off target, which KB nodes relate, what actions to take, and which owners or metrics to follow.";
          }
          function syncAssistantInputPlaceholder() {
            if (!assistantInput) return;
            if (!assistantState.selectedKpiId) {
              assistantInput.placeholder = "Ask about KPIs or quote delays";
              return;
            }
            const card = getKpiCardById(assistantState.selectedKpiId);
            const kpiName = getAssistantKpiDisplayName(card);
            assistantInput.placeholder = "Ask about " + kpiName;
          }

          function openAssistant() {
            if (!assistantShell) return;
            assistantShell.classList.add("open");
            if (assistantLauncher) assistantLauncher.textContent = "×";
            if (assistantFocus) {
              if (!assistantState.selectedKpiId) {
                assistantFocus.textContent = "All KPIs on this form";
              } else {
                const card = getKpiCardById(assistantState.selectedKpiId);
                assistantFocus.textContent = "Focused on: " + getAssistantKpiDisplayName(card);
              }
            }
            syncAssistantInputPlaceholder();
            const greetingKey = assistantState.selectedKpiId || "all";
            if (!assistantState.booted || assistantState.greetingKey !== greetingKey) {
              addAssistantMessage("assistant", buildAssistantGreeting());
              assistantState.booted = true;
              assistantState.greetingKey = greetingKey;
            }
            if (assistantInput) assistantInput.focus();
          }
          function closeAssistant() {
            if (!assistantShell) return;
            assistantShell.classList.remove("open");
            if (assistantLauncher) assistantLauncher.textContent = "🤖";
          }
          function openAssistantForKpi(kvId) {
            if (assistantState.selectedKpiId !== kvId) {
              resetAssistantConversation();
            }
            assistantState.selectedKpiId = kvId;
            openAssistant();
            const card = getKpiCardById(kvId);
            const kpiName = getAssistantKpiDisplayName(card);
            setAssistantStatus("Focused on " + kpiName + ". This AI support is dedicated to this KPI and will use the knowledge base when relevant.");
          }
          window.openAssistantForKpi = openAssistantForKpi;

          async function sendAssistantPrompt(message) {
            const cleanMessage = String(message || "").trim();
            if (!cleanMessage || assistantState.pending) return;
            assistantState.pending = true;
            if (assistantSend) assistantSend.disabled = true;
            addAssistantMessage("user", cleanMessage);
            setAssistantStatus("Thinking...");
            try {
              const res = await fetch("/kpi-ai-assistant", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  responsible_id: "${responsible_id}",
                  week: "${week}",
                  selected_kpi_id: assistantState.selectedKpiId,
                  kpis: collectAssistantKpis(),
                  message: cleanMessage
                })
              });
              const data = await res.json();
              if (!res.ok) throw new Error(data.error || "Request failed");
              addAssistantMessage("assistant", data.reply || "I could not generate a response.");
              setAssistantStatus("AI assistant is ready.");
            } catch (err) {
              addAssistantMessage("assistant", "I could not answer right now. Please try again.");
              setAssistantStatus("AI assistant is unavailable.");
            } finally {
              assistantState.pending = false;
              if (assistantSend) assistantSend.disabled = false;
            }
          }

          if (assistantLauncher) assistantLauncher.addEventListener("click", () => {
            if (assistantShell.classList.contains("open")) {
              closeAssistant();
              return;
            }
            if (assistantState.selectedKpiId !== null) {
              resetAssistantConversation();
            }
            assistantState.selectedKpiId = null;
            openAssistant();
          });
          if (assistantClose) assistantClose.addEventListener("click", closeAssistant);
          if (assistantInput) {
            assistantInput.addEventListener("input", () => {
              assistantInput.style.height = "auto";
              assistantInput.style.height = Math.min(assistantInput.scrollHeight, 140) + "px";
            });
            assistantInput.addEventListener("keydown", e => {
              if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); assistantForm && assistantForm.requestSubmit(); }
            });
          }
          if (assistantForm) assistantForm.addEventListener("submit", async e => {
            e.preventDefault();
            const prompt = getInputValue(assistantInput);
            if (assistantInput) { assistantInput.value = ""; assistantInput.style.height = "auto"; }
            await sendAssistantPrompt(prompt);
          });

          /* ═══════════════════════════════════════════════
             CHARTS
          ═══════════════════════════════════════════════ */
          const kpiCharts = {};
          let expandedChart = null;
          let expandedChartKpiValuesId = null;

          async function refreshKpiChartFromServer(kvId) {
  const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');
  if (!card) return;

  const responsibleId = new URLSearchParams(window.location.search).get("responsible_id");
  const week = card.dataset.currentWeek;
  const kpiId = card.dataset.kpiId;

  if (!responsibleId || !week || !kpiId) return;

  const url =
    "/api/kpi-chart-data?responsible_id=" + encodeURIComponent(responsibleId) +
    "&kpi_id=" + encodeURIComponent(kpiId) +
    "&week=" + encodeURIComponent(week);

  const res = await fetch(url);
  if (!res.ok) return;

  const data = await res.json();
  if (!Array.isArray(data.labels) || !Array.isArray(data.values)) return;

  card.dataset.historyLabels = JSON.stringify(data.labels);
  card.dataset.historyValues = JSON.stringify(data.values);
  if (data.currentMonthLabel) {
    card.dataset.currentMonthLabel = data.currentMonthLabel;
  }

  const input = document.getElementById("value_" + kvId);
  if (input && data.currentValue !== null) {
    input.value = data.currentValue;
  }

  const chart = kpiCharts[kvId];
  if (!chart) {
    buildKpiChart(kvId);
    return;
  }

  chart.data.labels = data.labels;
  chart.data.datasets[0].data = data.values;
  updateKpiChart(kvId);
}


         function startRealtimeCharts() {
  document.querySelectorAll(".kpi-card").forEach(card => {
    const kvId = card.dataset.kpiValuesId;
    if (!kvId) return;

    refreshKpiChartFromServer(kvId);

    setInterval(() => {
      refreshKpiChartFromServer(kvId);
    }, 30000);
  });
}
          function formatChartValue(value) {
            const n = Number(value);
            if (!isFinite(n)) return "";
            return n.toLocaleString("en-US", { minimumFractionDigits: Math.abs(n % 1) > 0.001 ? 2 : 0, maximumFractionDigits: 2 });
          }
          const expandedChartValueLabelsPlugin = {
            id: "expandedChartValueLabels",
            afterDatasetsDraw(chart) {
              const idx = chart.data.datasets.findIndex(d => (d.type || chart.config.type) !== "line");
              if (idx === -1) return;
              const dataset = chart.data.datasets[idx];
              const meta = chart.getDatasetMeta(idx);
              const ctx = chart.ctx;
              ctx.save();
              ctx.fillStyle = "#334155"; ctx.font = "600 13px Segoe UI"; ctx.textAlign = "center"; ctx.textBaseline = "bottom";
              meta.data.forEach((bar, i) => {
                const val = typeof dataset.data[i] === "number" ? dataset.data[i] : parseFloat(dataset.data[i]);
                if (isNaN(val)) return;
                const pt = bar.tooltipPosition();
                ctx.fillText(formatChartValue(val), pt.x, pt.y - 8);
              });
              ctx.restore();
            }
          };

          function hexToRgba(hex, alpha) {
            const n = String(hex || "").replace("#", "");
            if (n.length !== 6) return "rgba(148,163,184," + alpha + ")";
            return "rgba(" + parseInt(n.slice(0,2),16) + "," + parseInt(n.slice(2,4),16) + "," + parseInt(n.slice(4,6),16) + "," + alpha + ")";
          }

          function getSeparatedLines(lines, top, bottom, minSpc) {
            if (!lines.length) return [];
            const positioned = lines
              .map(l => ({ ...l, displayY: Math.min(Math.max(l.actualY, top + minSpc/2), bottom - minSpc/2) }))
              .sort((a,b) => a.displayY - b.displayY);
            const anchor = Math.max(0, positioned.findIndex(l => l.key === "target"));
            positioned[anchor].displayY = Math.min(Math.max(positioned[anchor].actualY - 2, top + minSpc/2), bottom - minSpc/2);
            for (let i = anchor - 1; i >= 0; i--) { positioned[i].displayY = Math.min(positioned[i].actualY, positioned[i+1].displayY - minSpc); }
            for (let i = anchor + 1; i < positioned.length; i++) { positioned[i].displayY = Math.max(positioned[i].actualY, positioned[i-1].displayY + minSpc); }
            const overflow = positioned[positioned.length-1].displayY - (bottom - minSpc/2);
            if (overflow > 0) positioned.forEach(l => l.displayY -= overflow);
            for (let i = positioned.length - 2; i >= 0; i--) { if (positioned[i].displayY > positioned[i+1].displayY - minSpc) positioned[i].displayY = positioned[i+1].displayY - minSpc; }
            const underflow = (top + minSpc/2) - positioned[0].displayY;
            if (underflow > 0) positioned.forEach(l => l.displayY += underflow);
            return positioned;
          }

          const kpiThresholdLinesPlugin = {
            id: "kpiThresholdLines",
            afterDatasetsDraw(chart, args, opts) {
              const { left, right, top, bottom } = chart.chartArea;
              const yScale = chart.scales.y;
              const lines = (opts && opts.lines) || [];
              if (!lines.length) return;
              const ctx = chart.ctx;
              const visibleLines = lines.map(l => {
                const v = Number(l.value);
                if (!isFinite(v)) return null;
                const ay = yScale.getPixelForValue(v);
                if (!isFinite(ay) || ay < top || ay > bottom) return null;
                return { ...l, value: v, actualY: ay };
              }).filter(Boolean);
              const rendered = getSeparatedLines(visibleLines, top, bottom, 18);
              ctx.save();
              rendered.forEach(l => {
                const y = l.displayY;
                if (!isFinite(y) || y < top || y > bottom) return;
                if (Math.abs(l.displayY - l.actualY) > 1) {
                  ctx.beginPath(); ctx.setLineDash([2,3]); ctx.lineWidth = 1;
                  ctx.strokeStyle = hexToRgba(l.borderColor, 0.45);
                  ctx.moveTo(right - 20, l.actualY); ctx.lineTo(right - 3, l.displayY); ctx.stroke();
                }
                ctx.beginPath(); ctx.setLineDash(l.borderDash || [6,4]);
                ctx.lineWidth = l.borderWidth || 2; ctx.strokeStyle = l.borderColor;
                ctx.moveTo(left, y); ctx.lineTo(right, y); ctx.stroke();
              });
              ctx.restore();
            }
          };

          function getThresholdLines(lowLimit, target, highLimit) {
            const lines = [];
            if (!isNaN(highLimit)) lines.push({ key:"high_limit", label:"High", value:highLimit, borderColor:"#f59e0b", borderDash:[10,4], borderWidth:2 });
            if (!isNaN(target)) lines.push({ key:"target", label:"Target", value:target, borderColor:"#22c55e", borderDash:[], borderWidth:2.5 });
            if (!isNaN(lowLimit)) lines.push({ key:"low_limit", label:"Low", value:lowLimit, borderColor:"#ef4444", borderDash:[4,4], borderWidth:2 });
            return lines;
          }

          function getPointColor(value, lowLimit, highLimit, direction) {
            const val = parseFloat(value);
            const rawLow = parseFloat(lowLimit), rawHigh = parseFloat(highLimit);
            const hasLow = !isNaN(rawLow), hasHigh = !isNaN(rawHigh);
            const lower = hasLow && hasHigh ? Math.min(rawLow,rawHigh) : hasLow ? rawLow : hasHigh ? rawHigh : null;
            const upper = hasLow && hasHigh ? Math.max(rawLow,rawHigh) : hasHigh ? rawHigh : hasLow ? rawLow : null;
            const dir = String(direction||"up").toLowerCase() === "down" ? "down" : "up";
            if (isNaN(val)) return "#6b7280";
            if (dir === "down") return (upper !== null && val > upper) ? "#ef4444" : "#22c55e";
            return (lower !== null && val < lower) ? "#ef4444" : "#22c55e";
          }

          function computeBounds(values, lowLimit, target, highLimit) {
            const all = values.filter(v => !isNaN(v) && v !== null);
            if (!isNaN(lowLimit)) all.push(lowLimit);
            if (!isNaN(target)) all.push(target);
            if (!isNaN(highLimit)) all.push(highLimit);
            const mn = all.length ? Math.min(...all) : 0;
            const mx = all.length ? Math.max(...all) : 100;
            const spread = Math.max(mx - mn, 1);
            const pad = Math.max(spread * 0.15, 10);
            return { min: mn - pad, max: mx + pad };
          }

          function buildKpiChart(kvId) {
            const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');
            if (!card) return;
            const canvas = document.getElementById("chart_" + kvId);
            if (!canvas) return;
            const lowLimit = parseFloat(card.dataset.lowLimit);
            const highLimit = parseFloat(card.dataset.highLimit);
            const target = parseFloat(card.dataset.target);
            const dir = card.dataset.goodDirection || "up";
            let labels = [], values = [];
            try { labels = JSON.parse(card.dataset.historyLabels || "[]"); values = JSON.parse(card.dataset.historyValues || "[]"); } catch(e){}
            const colors = values.map(v => getPointColor(v, lowLimit, highLimit, dir));
            const bounds = computeBounds(values, lowLimit, target, highLimit);
            kpiCharts[kvId] = new Chart(canvas.getContext("2d"), {
              type:"bar",
              data:{ labels, datasets:[{ label:"Value", data:values, backgroundColor:colors, borderRadius:6, borderSkipped:false, barThickness:30 }] },
              options:{
                responsive:true, maintainAspectRatio:false, animation:false,
                layout:{ padding:{ right:12 } },
                plugins:{ legend:{ display:false }, kpiThresholdLines:{ lines: getThresholdLines(lowLimit,target,highLimit) } },
                scales:{ x:{ grid:{ display:false } }, y:{ min:bounds.min, max:bounds.max } }
              },
              plugins:[kpiThresholdLinesPlugin]
            });
          }

          function updateKpiChart(kvId) {
            const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');
            if (!card) return;
            const chart = kpiCharts[kvId]; if (!chart) return;
            const lowLimit = parseFloat(card.dataset.lowLimit), highLimit = parseFloat(card.dataset.highLimit), target = parseFloat(card.dataset.target);
            const dir = card.dataset.goodDirection || "up";
            const data = chart.data.datasets[0].data;
            const colors = data.map(v => getPointColor(v, lowLimit, highLimit, dir));
            chart.data.datasets[0].backgroundColor = colors;
            const bounds = computeBounds(data, lowLimit, target, highLimit);
            chart.options.scales.y.min = bounds.min; chart.options.scales.y.max = bounds.max;
            chart.options.plugins.kpiThresholdLines.lines = getThresholdLines(lowLimit, target, highLimit);
            chart.update();
            if (expandedChartKpiValuesId === kvId && expandedChart) syncExpandedChart(chart);
          }

         function weekLabelToDateClient(weekStr) {
  const m = String(weekStr || "").match(/^(\d{4})-Week(\d{1,2})$/);
  if (!m) return null;

  const year = parseInt(m[1], 10);
  const weekNum = parseInt(m[2], 10);
  return new Date(year, 0, 1 + (weekNum - 1) * 7);
}

function weekToMonthLabelClient(weekStr) {
  const d = weekLabelToDateClient(weekStr);
  if (!d || isNaN(d.getTime())) return "";
  return d.toLocaleString("en-US", { month: "short", year: "numeric" });
}

function getFallbackCurrentMonthLabel(card, labels) {
  const storedMonthLabel = String(card?.dataset?.currentMonthLabel || "").trim();
  if (storedMonthLabel) return storedMonthLabel;

  const weekFromCard = String(card?.dataset?.currentWeek || "").trim();
  const monthFromCardWeek = weekToMonthLabelClient(weekFromCard);
  if (monthFromCardWeek) return monthFromCardWeek;

  const pageWeek = new URLSearchParams(window.location.search).get("week") || "";
  const monthFromPageWeek = weekToMonthLabelClient(pageWeek);
  if (monthFromPageWeek) return monthFromPageWeek;

  const browserMonthLabel = new Date().toLocaleString("en-US", { month: "short", year: "numeric" });
  if (Array.isArray(labels) && labels.includes(browserMonthLabel)) return browserMonthLabel;

  if (Array.isArray(labels) && labels.length) {
    return labels[labels.length - 1];
  }

  return browserMonthLabel;
}


          function updateCurrentMonthBarFromInput(kvId, rawValue) {
  const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');
  const chart = kpiCharts[kvId];
  if (!card || !chart) return;

  const value = parseFloat(rawValue);

  let labels = [];
  let values = [];

  try {
    labels = JSON.parse(card.dataset.historyLabels || "[]");
    values = JSON.parse(card.dataset.historyValues || "[]");
  } catch (e) {
    labels = [];
    values = [];
  }

  const currentMonthLabel = getFallbackCurrentMonthLabel(card, labels);
  if (!currentMonthLabel) return;

  const existingIndex = labels.indexOf(currentMonthLabel);

  if (isNaN(value)) {
    if (existingIndex >= 0) {
      values[existingIndex] = null;
    }
  } else {
    if (existingIndex >= 0) {
      values[existingIndex] = value;
    } else {
      labels.push(currentMonthLabel);
      values.push(value);
    }
  }

  card.dataset.historyLabels = JSON.stringify(labels);
  card.dataset.historyValues = JSON.stringify(values);

  chart.data.labels = labels.slice();
  chart.data.datasets[0].data = values.slice();

  updateKpiChart(kvId);
}

          function syncExpandedChart(src) {
            if (!expandedChart || !src) return;
            expandedChart.data.labels = src.data.labels.slice();
            expandedChart.data.datasets = src.data.datasets.map((d,i) => {
              const c = Object.assign({}, d);
              c.data = d.data.slice(); c.backgroundColor = Array.isArray(d.backgroundColor) ? d.backgroundColor.slice() : d.backgroundColor;
              if (i===0){ c.barThickness=56; c.maxBarThickness=72; c.borderRadius=10; }
              return c;
            });
            expandedChart.options.scales.y.min = src.options.scales.y.min;
            expandedChart.options.scales.y.max = src.options.scales.y.max;
            expandedChart.options.plugins.kpiThresholdLines.lines = (src.options.plugins.kpiThresholdLines.lines||[]).map(l=>({...l,borderDash:(l.borderDash||[]).slice()}));
            expandedChart.update();
          }

          function closeChartModal() {
            if (expandedChart) { expandedChart.destroy(); expandedChart = null; }
            expandedChartKpiValuesId = null;
            const chartModal = document.getElementById("chartModal");
            if (chartModal) { chartModal.classList.remove("active"); chartModal.setAttribute("aria-hidden","true"); }
            if (!document.querySelector(".history-modal-overlay.active") && !document.querySelector(".ca-modal-overlay.active")) {
              document.body.classList.remove("chart-modal-open");
            }
          }

          function openChartModal(kvId) {
            const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');
            const chartModal = document.getElementById("chartModal");
            const chartModalCanvas = document.getElementById("chartModalCanvas");
            const chartModalTitle = document.getElementById("chartModalTitle");
            const chartModalSubtitle = document.getElementById("chartModalSubtitle");
            if (!card || !chartModal || !chartModalCanvas) return;
            if (!kpiCharts[kvId]) buildKpiChart(kvId);
            const src = kpiCharts[kvId]; if (!src) return;
            if (chartModalTitle) chartModalTitle.textContent = getTrimmedText(card.querySelector(".kpi-title")) || "KPI Trend";
            if (chartModalSubtitle) chartModalSubtitle.textContent = getTrimmedText(card.querySelector(".kpi-subtitle")) || "Monthly KPI performance overview";
            if (expandedChart) { expandedChart.destroy(); expandedChart = null; }
            expandedChartKpiValuesId = kvId;
            chartModal.classList.add("active"); chartModal.setAttribute("aria-hidden","false");
            document.body.classList.add("chart-modal-open");
            requestAnimationFrame(() => {
              if (!chartModal.classList.contains("active")) return;
              expandedChart = new Chart(chartModalCanvas.getContext("2d"), {
                type:"bar",
                data:{
                  labels: src.data.labels.slice(),
                  datasets: src.data.datasets.map((d,i) => {
                    const c = Object.assign({},d); c.data = d.data.slice(); c.backgroundColor = Array.isArray(d.backgroundColor)?d.backgroundColor.slice():d.backgroundColor;
                    if(i===0){c.barThickness=56;c.maxBarThickness=72;c.borderRadius=10;} return c;
                  })
                },
                options:{
                  responsive:true, maintainAspectRatio:false, animation:false,
                  interaction:{ mode:"index", intersect:false }, layout:{ padding:{right:12} },
                  plugins:{ legend:{display:false}, kpiThresholdLines:{ lines:(src.options.plugins.kpiThresholdLines.lines||[]).map(l=>({...l,borderDash:(l.borderDash||[]).slice()})) } },
                  scales:{ x:{grid:{display:false},ticks:{color:"#475569",font:{size:14,weight:"600"}}}, y:{min:src.options.scales.y.min,max:src.options.scales.y.max,ticks:{color:"#64748b",font:{size:13}},grid:{color:"#e2e8f0"}} }
                },
                plugins:[expandedChartValueLabelsPlugin, kpiThresholdLinesPlugin]
              });
            });
          }

          /* ── History modal helpers ── */
          function escapeHistoryHtml(v) { return String(v||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#39;"); }
          function formatHistoryText(v) { return escapeHistoryHtml(v).split("\\n").join("<br>"); }
          function decodeModalPayload(v, fb) { try { const d=decodeURIComponent(v||""); if(!d) return fb; const p=JSON.parse(d); return (p===null||p===undefined)?fb:p; } catch(e){ return fb; } }
          function formatHistoryWeek(w) { const m=String(w||"").match(/^(\\d{4})-Week(\\d{1,2})$/); return m?"Week "+parseInt(m[2],10):escapeHistoryHtml(w||""); }
          function getHistoryStatusClass(s) { return "status-"+String(s||"Open").trim().toLowerCase().replace(/\s+/g,"-"); }

          function closeHistoryModal() {
            const historyModal = document.getElementById("historyModal");
            if (!historyModal) return;
            historyModal.classList.remove("active"); historyModal.setAttribute("aria-hidden","true");
            if (!document.querySelector(".chart-modal-overlay.active") && !document.querySelector(".ca-modal-overlay.active")) {
              document.body.classList.remove("chart-modal-open");
            }
          }

        function openHistoryModal(kvId) {
  const historyModal = document.getElementById("historyModal");
  const historyModalTitle = document.getElementById("historyModalTitle");
  const historyModalSubtitle = document.getElementById("historyModalSubtitle");
  const historyModalContent = document.getElementById("historyModalContent");
  const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');

  if (!card || !historyModal || !historyModalContent) return;

  const titleText = getTrimmedText(card.querySelector(".kpi-title")) || "Corrective Action History";
  const subtitleText = getTrimmedText(card.querySelector(".kpi-subtitle"));
  const prevLabel = card.dataset.prevMonthLabel || "";
  const prevActions = decodeModalPayload(card.dataset.prevMonthActions, []);
  const prevComments = decodeModalPayload(card.dataset.prevMonthComments, []);
  const comments = Array.isArray(prevComments) ? prevComments : [];
  const actions = Array.isArray(prevActions) ? prevActions : [];

  if (historyModalTitle) historyModalTitle.textContent = titleText;
  if (historyModalSubtitle) {
    historyModalSubtitle.textContent = prevLabel
      ? prevLabel + (subtitleText ? " • " + subtitleText : "")
      : "No previous month data available";
  }

  const chips = [];

let actionsHtml = "";
if (actions.length) {
  actionsHtml =
    '<div class="history-table-wrap">' +
      '<table class="history-table">' +
        '<thead>' +
          '<tr>' +
            '<th>#</th>' +
            '<th>Week</th>' +
            '<th>Root Cause</th>' +
            '<th>Implemented Solution</th>' +
            '<th>Due Date</th>' +
            '<th>Responsible</th>' +
            '<th>Status</th>' +
          '</tr>' +
        '</thead>' +
        '<tbody>' +
          actions.map(function(action, index) {
            return '' +
              '<tr>' +
                '<td>' + (index + 1) + '</td>' +
                '<td>' + escapeHistoryHtml(action.week || "") + '</td>' +
                '<td><pre>' + escapeHistoryHtml(action.root_cause || "") + '</pre></td>' +
                '<td><pre>' + escapeHistoryHtml(action.implemented_solution || "") + '</pre></td>' +
                '<td>' + escapeHistoryHtml(action.due_date || "") + '</td>' +
                '<td>' + escapeHistoryHtml(action.responsible || "") + '</td>' +
                '<td>' +
                  (action.status
                    ? '<span class="history-chip ' + getHistoryStatusClass(action.status) + '">' + escapeHistoryHtml(action.status) + '</span>'
                    : '—') +
                '</td>' +
              '</tr>';
          }).join("") +
        '</tbody>' +
      '</table>' +
    '</div>';
} else {
  actionsHtml = '<div class="history-empty">No corrective actions were saved for this KPI in the previous month.</div>';
}

  const commentsHtml = comments.length
    ? '<div class="history-comments-list">' + comments.map(c =>
        '<div class="history-comment-card">' +
          '<div class="history-comment-label">' +
            escapeHistoryHtml(c.month_label || prevLabel || "") +
            (c.week ? ' • ' + formatHistoryWeek(c.week) : '') +
          '</div>' +
          '<div class="history-comment-text">' + formatHistoryText(c.text || "") + '</div>' +
        '</div>'
      ).join("") + '</div>'
    : '<div class="history-empty">No comments were saved for this KPI in the previous month.</div>';

  if (!actions.length && !comments.length) {
    historyModalContent.innerHTML =
      '<div class="history-empty">No previous-month corrective action or comment history was found for this KPI.</div>';
  } else {
    historyModalContent.innerHTML =
      (chips.length ? '<div class="history-meta-row">' + chips.join("") + '</div>' : '') +
      '<div class="history-section">' +
        '<h4 class="history-section-title">⚠️ Corrective Actions</h4>' +
        actionsHtml +
      '</div>';
  }

  historyModal.classList.add("active");
  historyModal.setAttribute("aria-hidden", "false");
  document.body.classList.add("chart-modal-open");
}

          /* ═══════════════════════════════════════════════
             DOM READY
          ═══════════════════════════════════════════════ */
        document.addEventListener("DOMContentLoaded", () => {
  // Value inputs
  document.querySelectorAll(".value-input").forEach(input => {
    const kvId = input.dataset.kpiValuesId;
    checkLowLimit(input);
    buildKpiChart(kvId);

    input.addEventListener("input", function() {
      checkLowLimit(this);
      updateCurrentMonthBarFromInput(kvId, this.value);
    });
  });

  startRealtimeCharts();
            // Chart expand
            document.querySelectorAll(".kpi-chart-trigger").forEach(panel => {
              const kvId = panel.dataset.kpiValuesId;
              if (!kvId) return;
              panel.addEventListener("click", () => openChartModal(kvId));
              panel.addEventListener("keydown", e => { if (e.key==="Enter"||e.key===" ") { e.preventDefault(); openChartModal(kvId); } });
            });
            document.querySelectorAll(".chart-expand-btn").forEach(btn => {
              const kvId = btn.dataset.kpiValuesId; if (!kvId) return;
              btn.addEventListener("click", e => { e.stopPropagation(); openChartModal(kvId); });
            });

            // History modal (view previous CA inside card panel)
            document.querySelectorAll(".view-ca-btn").forEach(btn => {
              const kvId = btn.dataset.kpiValuesId; if (!kvId) return;
              btn.addEventListener("click", () => openHistoryModal(kvId));
            });

            // Card action bar — View Previous Actions
            document.querySelectorAll(".view-prev-ca-btn").forEach(btn => {
              const kvId = btn.dataset.kpiValuesId; if (!kvId) return;
              btn.addEventListener("click", () => openHistoryModal(kvId));
            });

            // Card action bar — Open CA Table Modal
            document.querySelectorAll(".open-ca-modal-btn").forEach(btn => {
              const kvId = btn.dataset.kpiValuesId; if (!kvId) return;
              btn.addEventListener("click", () => openCaTableModal(kvId));
            });

            // Bind existing CA action cards
            document.querySelectorAll(".ca-actions-stack").forEach(stack => {
              const kvId = stack.dataset.kpiValuesId; if (!kvId) return;
              getCorrectiveActionCards(kvId).forEach(card => bindCorrectiveActionCard(card, kvId));
              renumberCorrectiveActionCards(kvId);
            });

            // Chart modal close
            const chartModal = document.getElementById("chartModal");
            const chartModalClose = document.getElementById("chartModalClose");
            if (chartModal && chartModalClose) {
              chartModalClose.addEventListener("click", closeChartModal);
              chartModal.addEventListener("click", e => { if (e.target===chartModal) closeChartModal(); });
            }

            // History modal close
            const historyModal = document.getElementById("historyModal");
            const historyModalClose = document.getElementById("historyModalClose");
            if (historyModal && historyModalClose) {
              historyModalClose.addEventListener("click", closeHistoryModal);
              historyModal.addEventListener("click", e => { if (e.target===historyModal) closeHistoryModal(); });
            }

            // CA Table Modal close
            const caModalClose = document.getElementById("caModalClose");
            const caTableModal = document.getElementById("caTableModal");
            if (caModalClose) caModalClose.addEventListener("click", closeCaTableModal);
            if (caTableModal) caTableModal.addEventListener("click", e => { if (e.target===caTableModal) closeCaTableModal(); });

            // CA modal form controls
            const caModalAddRowBtn = document.getElementById("caModalAddRowBtn");
            if (caModalAddRowBtn) caModalAddRowBtn.addEventListener("click", () => caModalOpenForm(null));
            const caModalFormCollapse = document.getElementById("caModalFormCollapse");
            if (caModalFormCollapse) caModalFormCollapse.addEventListener("click", caModalCollapseForm);
            const caModalCancelForm = document.getElementById("caModalCancelForm");
            if (caModalCancelForm) caModalCancelForm.addEventListener("click", caModalCollapseForm);
            const caModalSaveFormBtn = document.getElementById("caModalSaveForm");
            if (caModalSaveFormBtn) caModalSaveFormBtn.addEventListener("click", caModalSaveForm);

            // Escape key
            document.addEventListener("keydown", e => {
              if (e.key === "Escape") {
                if (document.getElementById("caTableModal").classList.contains("active")) { closeCaTableModal(); return; }
                if (document.getElementById("chartModal").classList.contains("active")) { closeChartModal(); return; }
                if (document.getElementById("historyModal").classList.contains("active")) { closeHistoryModal(); return; }
              }
            });

            // Submit form
            const form = document.getElementById("kpiForm");
            const confirmModal = document.getElementById("confirmModal");
            const confirmBtn = document.getElementById("confirmBtn");
            const cancelBtn = document.getElementById("cancelBtn");
            const loadingOverlay = document.getElementById("loadingOverlay");
            let formToSubmit = null;

            if (form && confirmModal && confirmBtn && cancelBtn) {
              form.addEventListener("submit", function(e) {
                e.preventDefault();
                let hasError = false;
                this.querySelectorAll("input.value-input[required]").forEach(input => {
                  if (!input.value.trim() || isNaN(input.value.trim())) { hasError = true; input.style.borderColor = "#dc3545"; }
                  else input.style.borderColor = "#ddd";
                });
                document.querySelectorAll(".ca-container.visible").forEach(panel => {
                  panel.querySelectorAll(".ca-textarea, .ca-date-input, .ca-text-input").forEach(field => {
                    if (!field.value.trim()) { hasError = true; field.classList.add("error"); }
                    else field.classList.remove("error");
                  });
                });
                if (hasError) return;
                formToSubmit = this;
                confirmModal.classList.add("active");
              });
              confirmBtn.addEventListener("click", () => {
                confirmModal.classList.remove("active");
                if (loadingOverlay) loadingOverlay.classList.add("active");
                if (formToSubmit) formToSubmit.submit();
              });
              cancelBtn.addEventListener("click", () => confirmModal.classList.remove("active"));
            }
          });

          window.caModalOpenEdit = caModalOpenEdit;
          window.caModalDeleteAction = caModalDeleteAction;
        </script>
      </body>
      </html>
    `);
  } catch (err) {
    console.error("Error loading form:", err);
    res.send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});


// ---------- Dashboard ----------
app.get("/dashboard", async (req, res) => {
  try {
    const { responsible_id } = req.query;

    const resResp = await pool.query(
      `SELECT r.*, p.name AS plant_name, d.name AS department_name
       FROM public."Responsible" r
       JOIN public."Plant" p ON r.plant_id = p.plant_id
       JOIN public."Department" d ON r.department_id = d.department_id
       WHERE r.responsible_id = $1`,
      [responsible_id]
    );

    const responsible = resResp.rows[0];
    if (!responsible) throw new Error("Responsible not found");

    const kpiRes = await pool.query(
      `SELECT DISTINCT ON (h.week, h.kpi_id)
              h.hist_id,
              h.kpi_values_id,
              h.new_value as value,
              h.week,
              h.kpi_id,
              h.updated_at,
              k.subject,
              k.indicator_sub_title,
              k.unit,
              k.target,
              k.min,
              k.max,
              k.tolerance_type,
              k.up_tolerance,
              k.low_tolerance,
              k.frequency,
              k.definition,
              k.calculation_on,
              k.target_auto_adjustment,
              k.high_limit,
              k.low_limit
       FROM public.kpi_values_hist26 h
       JOIN public."Kpi" k ON h.kpi_id = k.kpi_id
       WHERE h.responsible_id = $1
       ORDER BY h.week DESC, h.kpi_id ASC, h.updated_at DESC`,
      [responsible_id]
    );

    const monthMap = new Map();

    kpiRes.rows.forEach((kpi) => {
      if (!kpi.updated_at) return;

      const date = new Date(kpi.updated_at);
      const monthKey = `${date.getFullYear()}-${date.getMonth()}`;

      if (!monthMap.has(monthKey)) monthMap.set(monthKey, []);
      monthMap.get(monthKey).push(kpi);
    });

    const sortedMonthEntries = Array.from(monthMap.entries()).sort((a, b) => {
      const [yearA, monthA] = a[0].split("-").map(Number);
      const [yearB, monthB] = b[0].split("-").map(Number);
      return new Date(yearB, monthB) - new Date(yearA, monthA);
    });

    let html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>KPI Dashboard</title>
  <style>
    body{
      font-family:'Segoe UI',sans-serif;
      background:#f4f6f9;
      background-image:url('https://images.unsplash.com/photo-1486262715619-67b85e0b08d3?w=1600');
      background-size:cover;
      background-position:center;
      background-attachment:fixed;
      padding:20px;
      margin:0;
    }
    .container{max-width:900px;margin:0 auto;}
    .header{
      background:#0078D7;
      color:white;
      padding:20px;
      text-align:center;
      border-radius:8px 8px 0 0;
    }
    .content{
      background:#fff;
      padding:30px;
      border-radius:0 0 8px 8px;
      box-shadow:0 2px 10px rgba(0,0,0,0.1);
    }
    .info-section{
      background:#f8f9fa;
      padding:20px;
      border-radius:6px;
      margin-bottom:25px;
      border-left:4px solid #0078D7;
    }
    .info-row{display:flex;margin-bottom:10px;}
    .info-label{width:120px;font-weight:600;color:#333;}
    .info-value{
      flex:1;
      background:white;
      padding:8px 12px;
      border:1px solid #ddd;
      border-radius:4px;
    }
    .month-section{
      margin-bottom:30px;
      border:1px solid #e1e5e9;
      border-radius:8px;
      padding:20px;
      background:#fafbfc;
    }
    .month-title{
      color:#0078D7;
      font-size:20px;
      margin-bottom:15px;
      font-weight:600;
      border-bottom:2px solid #0078D7;
      padding-bottom:8px;
    }
    .kpi-card{
      background:#fff;
      border:1px solid #e1e5e9;
      border-radius:6px;
      padding:15px;
      margin-bottom:15px;
    }
    .kpi-title{
      font-weight:600;
      color:#333;
      margin-bottom:5px;
      font-size:16px;
    }
    .kpi-date{
      color:#999;
      font-size:11px;
      margin-top:3px;
      font-style:italic;
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>KPI Dashboard - ${responsible.name}</h1>
    </div>

    <div class="content">
      <div class="info-section">
        <div class="info-row">
          <div class="info-label">Responsible</div>
          <div class="info-value">${responsible.name}</div>
        </div>
        <div class="info-row">
          <div class="info-label">Group</div>
          <div class="info-value">${responsible.plant_name}</div>
        </div>
        <div class="info-row">
          <div class="info-label">Department</div>
          <div class="info-value">${responsible.department_name}</div>
        </div>
      </div>`;

    if (sortedMonthEntries.length === 0) {
      html += `<div style="color:#999;font-style:italic;">No KPI data available yet.</div>`;
    } else {
      for (const [monthKey, items] of sortedMonthEntries) {
        const [year, month] = monthKey.split("-").map(Number);
        const date = new Date(year, month);
        const monthName = date.toLocaleString("en-GB", { month: "long" });
        const monthLabel = `${monthName}-${year}`;

        html += `<div class="month-section"><div class="month-title">📅 ${monthLabel}</div>`;

        items.forEach((kpi) => {
          const hasValue = kpi.value !== null && kpi.value !== undefined && kpi.value !== '';
          const dotColor = hasValue
            ? getDotColor(kpi.value, kpi.low_limit, kpi.high_limit, inferKpiDirection(kpi))
            : '#6c757d';

          const submitted = kpi.updated_at
            ? new Date(kpi.updated_at).toLocaleString('en-GB', {
              day: '2-digit',
              month: 'short',
              year: 'numeric',
              hour: '2-digit',
              minute: '2-digit'
            })
            : '';

          html += `
            <div class="kpi-card">
              <div class="kpi-title">${kpi.subject}</div>
              ${kpi.indicator_sub_title
              ? `<div style="color:#666;font-size:13px;font-style:italic;">${kpi.indicator_sub_title}</div>`
              : ''}

              <div style="display:flex;justify-content:space-between;align-items:center;margin:12px 0;">
                ${kpi.target
              ? `<div>
                      <span style="font-weight:600;color:#495057;">Target: </span>
                      <span style="color:#28a745;font-weight:700;">
                        ${parseFloat(kpi.target).toLocaleString()} ${kpi.unit || ''}
                      </span>
                    </div>`
              : ''}

                <div>
                  <span style="font-weight:600;color:#495057;">Actual: </span>
                  <span style="font-size:20px;font-weight:700;color:${dotColor};">
                    ${hasValue ? kpi.value : 'Not filled'} ${kpi.unit || ''}
                  </span>
                </div>
              </div>

              ${kpi.high_limit
              ? `<div style="font-size:12px;color:#ff9800;">🔺 High Limit: ${kpi.high_limit} ${kpi.unit || ''}</div>`
              : ''}

              ${kpi.low_limit
              ? `<div style="font-size:12px;color:#dc3545;">🔻 Low Limit: ${kpi.low_limit} ${kpi.unit || ''}</div>`
              : ''}

              ${submitted
              ? `<div class="kpi-date">Last updated: ${submitted}</div>`
              : ''}
            </div>`;
        });

        html += `</div>`;
      }
    }

    html += `
    </div>
  </div>
</body>
</html>`;

    res.send(html);
  } catch (err) {
    res.status(500).send(`<h2 style="color:red;">Error: ${err.message}</h2>`);
  }
});

app.get("/dashboard-history", async (req, res) => {
  try {
    const { responsible_id } = req.query;
    const histRes = await pool.query(
      `SELECT h.hist_id, h.kpi_id, h.week, h.old_value, h.new_value, h.updated_at,
              k.subject, k.indicator_sub_title, k.unit
       FROM public.kpi_values_hist26 h JOIN public."Kpi" k ON h.kpi_id = k.kpi_id
       WHERE h.responsible_id = $1 ORDER BY h.updated_at DESC`, [responsible_id]);
    const rows = histRes.rows;
    res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>KPI History</title>
      <style>body{font-family:'Segoe UI',sans-serif;background:#f4f6f9;padding:20px}
      .container{max-width:1000px;margin:0 auto;background:#fff;padding:30px;border-radius:8px}
      h1{color:#0078D7}table{width:100%;border-collapse:collapse;margin-top:20px}
      th,td{padding:10px 12px;border:1px solid #ddd;text-align:center}
      th{background:#0078D7;color:white}tr:nth-child(even){background:#f8f9fa}</style></head><body>
      <div class="container"><h1>KPI Value History</h1>
      <table><thead><tr><th>Indicator</th><th>Sub Title</th><th>Week</th>
        <th>Old Value</th><th>New Value</th><th>Unit</th><th>Updated At</th></tr></thead>
      <tbody>${rows.map(r => `<tr>
        <td>${r.subject}</td><td>${r.indicator_sub_title || '-'}</td><td>${r.week}</td>
        <td>${r.old_value ?? '—'}</td><td>${r.new_value ?? '—'}</td>
        <td>${r.unit || ''}</td><td>${new Date(r.updated_at).toLocaleString()}</td>
      </tr>`).join('')}</tbody></table></div></body></html>`);
  } catch (err) {
    res.send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});

// ---------- Send KPI email ----------
const sendKPIEmail = async (responsibleId, week) => {
  try {
    const { responsible } = await getResponsibleWithKPIs(responsibleId, week);
    const html = generateEmailHtml({ responsible, week });
    const transporter = createTransporter();
    await transporter.sendMail({
      from: '"Administration STS" <administration.STS@avocarbon.com>',
      to: responsible.email,
      subject: `KPI Form for ${responsible.name} - ${week}`,
      html,
    });
    console.log(`✅ Email sent to ${responsible.email}`);
  } catch (err) {
    console.error(`❌ Failed to send email to responsible ID ${responsibleId}:`, err.message);
  }
};

const formatNumber = (num) => {
  const n = parseFloat(num);
  if (Number.isInteger(n)) return n.toString();
  if (Math.abs(n - Math.round(n)) < 0.0001) return Math.round(n).toString();
  return n.toFixed(1);
};

// ============================================================
// generateVerticalBarChart — DOTS + HIGH/LOW LIMIT LINES
// ============================================================
const generateVerticalBarChart = (chartData) => {
  const {
    title, subtitle, unit, data, weekLabels, currentWeek,
    stats, target, low_limit, high_limit,
    direction,
    tolerance_type, up_tolerance, low_tolerance,
    frequency, definition, comments = [],
    correctiveActions = []
  } = chartData;

  const cleanHigh = (high_limit && high_limit !== 'None' && high_limit !== '' && !isNaN(parseFloat(high_limit)))
    ? parseFloat(high_limit) : null;
  const cleanLow = (low_limit && low_limit !== 'None' && low_limit !== '' && !isNaN(parseFloat(low_limit)))
    ? parseFloat(low_limit) : null;
  const cleanTarget = (target && target !== 'None' && target !== '' && !isNaN(parseFloat(target)))
    ? parseFloat(target) : null;

  if (!data || data.length === 0 || data.every(v => v <= 0)) {
    return `<table border="0" cellpadding="20" cellspacing="0" width="100%"
              style="margin:20px 0;background:white;border-radius:8px;border:1px solid #e0e0e0;">
      <tr><td>
        <h3 style="margin:0;color:#333;font-size:16px;">${title}</h3>
        ${subtitle ? `<p style="margin:5px 0 0;color:#666;font-size:14px;">${subtitle}</p>` : ''}
        <p style="margin:15px 0;color:#999;font-size:14px;">No data available</p>
      </td></tr></table>`;
  }

  const validData = data.filter(v => v > 0 && !isNaN(v));
  const values = data.slice(0, 12);

  const labels =
    (weekLabels || data.map((_, i) => `W${i + 1}`))
      .slice(0, 12);

  const fmt = (num) => {
    if (num === null || num === undefined || isNaN(num)) return '0';
    const w = Math.abs(num - Math.round(num)) < 0.0001;
    if (num >= 1e6) return (num / 1e6).toFixed(w ? 0 : 1) + 'M';
    if (num >= 1e3) return (num / 1e3).toFixed(w ? 0 : 1) + 'k';
    if (w) return Math.round(num).toString();
    if (num < 1) return num.toFixed(2);
    return num.toFixed(1);
  };

  const resolvedDirection = normalizeKpiDirection(direction) || 'up';
  const pointColors = values.map(v => getDotColor(v, cleanLow, cleanHigh, resolvedDirection));

  const allVals = [...validData];
  if (cleanHigh !== null) allVals.push(cleanHigh);
  if (cleanLow !== null) allVals.push(cleanLow);
  if (cleanTarget !== null) allVals.push(cleanTarget);

  const dataMax = Math.max(...allVals, 1);
  const rawInt = dataMax / 10;
  const mag = Math.pow(10, Math.floor(Math.log10(rawInt || 1)));
  const norm = rawInt / mag;
  let interval;
  if (norm <= 1) interval = 1 * mag;
  else if (norm <= 2) interval = 2 * mag;
  else if (norm <= 2.5) interval = 2.5 * mag;
  else if (norm <= 5) interval = 5 * mag;
  else interval = 10 * mag;

  const maxValue = Math.ceil(dataMax / interval) * interval + interval;
  const numSteps = Math.round(maxValue / interval);
  const chartHeight = 180;
  const segmentHeight = chartHeight / numSteps;

  const yAxis = () => {
    let h = '';
    for (let i = numSteps; i >= 0; i--) {
      const val = i * interval;
      let ind = '';
      if (cleanHigh !== null && Math.abs(val - cleanHigh) < interval / 2) ind += ' 🔺';
      if (cleanLow !== null && Math.abs(val - cleanLow) < interval / 2) ind += ' 🔻';
      h += `<tr><td height="${segmentHeight}" valign="top" align="right"
              style="font-size:10px;color:#666;padding-right:8px;white-space:nowrap;">
              ${fmt(val)}${ind}</td></tr>`;
    }
    return h;
  };

  const currentValue = data[data.length - 1] || 0;

  // STATS BOX - 3 columns (CURRENT, AVERAGE, TREND)
  // Replace the statsBox const with this:
  const currentStatus = getKpiStatus(currentValue, cleanLow, cleanHigh, resolvedDirection);
  const trendIcon = currentStatus.isGood === false
    ? {
      icon: resolvedDirection === 'down' ? '↑' : '↓',
      color: '#dc2626'
    }
    : {
      icon: resolvedDirection === 'down' ? '↓' : '↑',
      color: '#28a745'
    };

  const statsBox = `
  <table border="0" cellpadding="0" cellspacing="0" width="100%"
         style="background:white;border-radius:12px;border:1px solid #e0e0e0;margin-bottom:20px;">
    <tr>
      <td style="padding:20px;">
        <table border="0" cellpadding="0" cellspacing="0" align="center"
               style="margin:0 auto;text-align:center;">
          <tr>

            <!-- CURRENT -->
            <td valign="middle" style="padding-right:30px;text-align:center;">
              <div style="font-size:11px;color:#666;text-transform:uppercase;margin-bottom:5px;">CURRENT</div>
              <div style="font-size:32px;font-weight:700;color:${getDotColor(currentValue, cleanLow, cleanHigh, resolvedDirection)};line-height:36px;">
                ${stats.current}
              </div>
              ${currentWeek ? `
                <div style="font-size:11px;color:#999;margin-top:2px;">
                  ${String(currentWeek).replace('2026-Week', 'Week ')}
                </div>` : ''}
            </td>

            <!-- TREND ICON — same row, between CURRENT and AVERAGE -->
            <td valign="middle" style="padding:0 30px;text-align:center;">
             <div style="font-size:11px;color:#666;text-transform:uppercase;margin-bottom:5px;">Direction</div>
              <div style="font-size:40px;font-weight:900;color:${trendIcon.color};line-height:1;">
                ${trendIcon.icon}
              </div>
           
            </td>

            <!-- AVERAGE -->
            <td valign="middle" style="padding-left:30px;text-align:center;">
              <div style="font-size:11px;color:#666;text-transform:uppercase;margin-bottom:5px;">AVERAGE</div>
              <div style="font-size:32px;font-weight:700;color:#0078D7;line-height:36px;">
                ${stats.average}
              </div>
              <div style="font-size:11px;color:#999;margin-top:2px;">
                ${stats.dataPoints || data.length} periods
              </div>
            </td>

          </tr>
        </table>
      </td>
    </tr>
  </table>
`;
  // QuickChart datasets
  const datasets = [
    {
      label: subtitle || title,
      data: values,
      borderColor: '#94a3b8',
      borderWidth: 2,
      lineTension: 0,
      fill: false,
      pointBackgroundColor: pointColors,
      pointBorderColor: '#ffffff',
      pointBorderWidth: 2,
      pointRadius: 6
    }
  ];

  if (cleanHigh !== null) {
    datasets.push({
      label: `High Limit (${fmt(cleanHigh)})`,
      data: new Array(values.length).fill(cleanHigh),
      borderColor: '#ff9800',
      borderWidth: 2,
      borderDash: [6, 4],
      pointRadius: 0,
      fill: false
    });
  }

  if (cleanLow !== null) {
    datasets.push({
      label: `Low Limit (${fmt(cleanLow)})`,
      data: new Array(values.length).fill(cleanLow),
      borderColor: '#dc3545',
      borderWidth: 2,
      borderDash: [6, 4],
      pointRadius: 0,
      fill: false
    });
  }

  if (cleanTarget !== null) {
    datasets.push({
      label: `Target (${fmt(cleanTarget)})`,
      data: new Array(values.length).fill(cleanTarget),
      borderColor: '#16a34a',
      borderWidth: 2,
      borderDash: [3, 3],
      pointRadius: 0,
      fill: false
    });
  }

  const chartConfig = {
    type: 'line',
    data: { labels, datasets },
    options: {
      legend: { display: false },
      scales: {
        xAxes: [{
          ticks: { fontSize: 10 },
          gridLines: { color: 'rgba(0,0,0,0.05)' }
        }],
        yAxes: [{
          ticks: { fontSize: 10, beginAtZero: false },
          gridLines: { color: 'rgba(0,0,0,0.05)' }
        }]
      }
    }
  };

  const chartUrl =
    `https://quickchart.io/chart?c=${encodeURIComponent(JSON.stringify(chartConfig))}` +
    `&w=500&h=260&bkg=white`;

  const commentsHtml = comments.length > 0 ? `
    <div style="margin-bottom:20px;">
      <h4 style="margin:0 0 15px;color:#333;font-size:16px;">💬 Comments</h4>
      ${comments.map(c => `
        <div style="margin-bottom:12px;padding:12px;background:#e3f2fd;border-radius:8px;border-left:4px solid #0078D7;">
          <div style="font-size:11px;font-weight:600;color:#0078D7;margin-bottom:6px;">
           ${weekToMonthLabel(c.week || '') || (c.week || '').replace('2026-Week', 'Week ')}
          </div>
          <div style="font-size:13px;color:#2c3e50;">${c.text}</div>
        </div>
      `).join('')}
    </div>` : '';

  const correctiveActionsHtml = correctiveActions.length > 0 ? `
    <div style="margin-bottom:20px;">
      <h4 style="margin:0 0 15px;color:#333;font-size:16px;">⚠️ Corrective Actions</h4>
      ${correctiveActions.map(ca => `
        <div style="margin-bottom:15px;padding:15px;background:#fff3f3;border-radius:8px;border-left:4px solid #dc3545;">
       <div style="font-size:12px;font-weight:600;color:#495057;margin-bottom:8px;">
         ${ca.week ? weekToMonthLabel(ca.week) : 'N/A'}
         ${ca.status ? `<span style="margin-left:10px;font-size:11px;color:#dc3545;">${ca.status}</span>` : ''}
          </div>
          ${ca.root_cause ? `
            <div style="margin-bottom:8px;">
              <div style="font-size:11px;font-weight:700;color:#dc3545;">🔍 Root Cause</div>
              <div style="font-size:12px;color:#374151;">${ca.root_cause}</div>
            </div>
          ` : ''}
          ${ca.implemented_solution ? `
            <div style="margin-bottom:8px;">
              <div style="font-size:11px;font-weight:700;color:#d97706;">⚡ Implemented Solution</div>
              <div style="font-size:12px;color:#374151;">${ca.implemented_solution}</div>
            </div>
          ` : ''}
          ${ca.evidence ? `
            <div>
              <div style="font-size:11px;font-weight:700;color:#2563eb;">📊 Evidence</div>
              <div style="font-size:12px;color:#374151;">${ca.evidence}</div>
            </div>
          ` : ''}
        </div>
      `).join('')}
    </div>
  ` : '';

  // ✅ EMAIL-SAFE LIMITS (High + Low side-by-side using table)
  const limitsRowHtml = (() => {
    const highBox = cleanHigh !== null ? `
    <table border="0" cellpadding="0" cellspacing="0" width="100%"
           style="background:white;border-radius:12px;border:1px solid #e0e0e0;text-align:center;">
      <tr><td style="padding:15px;">
        <div style="font-size:11px;color:#666;text-transform:uppercase;margin-bottom:5px;">🔺 HIGH LIMIT</div>
        <div style="font-size:28px;font-weight:700;color:#ff9800;">${fmt(cleanHigh)}</div>
        <div style="font-size:11px;color:#999;">${unit || ''}</div>
      </td></tr>
    </table>
  ` : '';

    const targetBox = cleanTarget !== null ? `
    <table border="0" cellpadding="0" cellspacing="0" width="100%"
           style="background:white;border-radius:12px;border:1px solid #e0e0e0;text-align:center;">
      <tr><td style="padding:15px;">
        <div style="font-size:11px;color:#666;text-transform:uppercase;margin-bottom:5px;">🎯 TARGET</div>
        <div style="font-size:28px;font-weight:700;color:#16a34a;">${fmt(cleanTarget)}</div>
        <div style="font-size:11px;color:#999;">${unit || ''}</div>
      </td></tr>
    </table>
  ` : '';

    const lowBox = cleanLow !== null ? `
    <table border="0" cellpadding="0" cellspacing="0" width="100%"
           style="background:white;border-radius:12px;border:1px solid #e0e0e0;text-align:center;">
      <tr><td style="padding:15px;">
        <div style="font-size:11px;color:#666;text-transform:uppercase;margin-bottom:5px;">🔻 LOW LIMIT</div>
        <div style="font-size:28px;font-weight:700;color:#dc3545;">${fmt(cleanLow)}</div>
        <div style="font-size:11px;color:#999;">${unit || ''}</div>
      </td></tr>
    </table>
  ` : '';

    const boxes = [];
    if (highBox) boxes.push(highBox);
    if (targetBox) boxes.push(targetBox);
    if (lowBox) boxes.push(lowBox);

    if (boxes.length === 0) {
      return `
      <div style="margin-top:20px;background:#f8f9fa;border-radius:12px;padding:30px;
                  text-align:center;border:1px dashed #e0e0e0;">
        <span style="font-size:24px;display:block;margin-bottom:10px;">📊</span>
        <p style="margin:0;color:#999;font-size:13px;">No limits defined</p>
      </div>
    `;
    }

    const colWidth = Math.floor(100 / boxes.length);

    return `
    <table border="0" cellpadding="0" cellspacing="0" width="100%" style="margin-top:20px;">
      <tr>
        ${boxes.map((box, i) => `
          <td width="${colWidth}%" valign="top"
              style="${i !== boxes.length - 1 ? 'padding-right:8px;' : ''}">
            ${box}
          </td>
        `).join('')}
      </tr>
    </table>
  `;
  })();

  return `
    <table border="0" cellpadding="0" cellspacing="0" width="100%"
           style="margin:20px 0;background:white;border-radius:12px;
                  border:1px solid #e0e0e0;font-family:Arial,sans-serif;">
      <tr><td style="padding:20px;">
        <div style="margin-bottom:20px;">
          <h3 style="margin:0;color:#333;font-size:18px;font-weight:600;">${title}</h3>
          ${subtitle ? `<p style="margin:5px 0 0;color:#666;font-size:14px;">${subtitle}</p>` : ''}
          ${unit ? `<p style="margin:5px 0 0;color:#888;font-size:12px;">Unit: ${unit} • Frequency: ${frequency || 'Monthly'}</p>` : ''}
        </div>

        ${statsBox}

        <table border="0" cellpadding="0" cellspacing="0" width="100%">
          <tr>
            <td width="60%" valign="top" style="padding-right:20px;">
              <table border="0" cellpadding="0" cellspacing="0" width="100%">
                <tr>
  
                  <td valign="top" style="padding-left:5px;">
                    <table border="0" cellpadding="0" cellspacing="0" width="100%" style="text-align:center;">
                      <tr>
                        <td align="center">
                          <img src="${chartUrl}"
                               width="500"
                               height="260"
                               alt="KPI Trend Chart"
                               style="max-width:100%;
                                      border-radius:8px;
                                      border:1px solid #f0f0f0;
                                      display:block;
                                      margin:auto;" />
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
              </table>

              <!-- ✅ High/Low limits beside each other -->
              ${limitsRowHtml}
            </td>

            <td width="40%" valign="top" style="padding-left:20px;border-left:2px solid #f0f0f0;">
              ${correctiveActionsHtml}
              ${commentsHtml}

              ${comments.length === 0 && correctiveActions.length === 0 ? `
                <div style="background:#f8f9fa;border-radius:12px;padding:30px;
                            text-align:center;border:1px dashed #e0e0e0;">
                  <span style="font-size:32px;display:block;margin-bottom:10px;">📝</span>
                  <p style="margin:0;color:#999;font-size:13px;">No additional data</p>
                </div>` : ''}
            </td>
          </tr>
        </table>
      </td></tr>
    </table>`;
};
// ============================================================
// generateWeeklyReportData
// ============================================================
// ============================================================
// generateWeeklyReportData (updated to include corrective actions)
// ============================================================
const generateWeeklyReportData = async (responsibleId, reportWeek) => {
  try {
    const histRes = await pool.query(
      `WITH KpiHistory AS (
        SELECT h.kpi_id, h.week, h.new_value, h.updated_at, h.comment,
               k.subject, k.indicator_sub_title, k.unit, k.target, k.min, k.max,
               k.tolerance_type, k.up_tolerance, k.low_tolerance, k.frequency, k.definition,
               k.low_limit, k.high_limit,
               ca.corrective_action_id,
               ca.root_cause,
               ca.implemented_solution,
               ca.evidence,
               ca.status as ca_status,
               ca.created_date as ca_created_date,
               ca.updated_date as ca_updated_date,
               ROW_NUMBER() OVER (PARTITION BY h.kpi_id, h.week ORDER BY h.updated_at DESC) as rn
        FROM public.kpi_values_hist26 h
        JOIN public."Kpi" k ON h.kpi_id = k.kpi_id
        LEFT JOIN LATERAL (
          SELECT corrective_action_id, root_cause, implemented_solution,
                 evidence, status, created_date, updated_date
          FROM public.corrective_actions
          WHERE kpi_id = h.kpi_id
            AND responsible_id = h.responsible_id
            AND week = h.week
          ORDER BY COALESCE(updated_date, created_date) DESC, corrective_action_id DESC
          LIMIT 1
        ) ca ON TRUE
        WHERE h.responsible_id = $1
          AND h.new_value IS NOT NULL AND h.new_value != '' AND h.new_value != '0'
      )
      SELECT * FROM KpiHistory WHERE rn = 1
      ORDER BY kpi_id, week ASC`,
      [responsibleId]
    );

    if (!histRes.rows.length) return null;

    const kpisData = {};

    // ── Pass 1: build kpisData entries and accumulate into monthly maps ──────
    histRes.rows.forEach(row => {
      const kpiId = row.kpi_id;
      if (!kpisData[kpiId]) {
        kpisData[kpiId] = {
          title: row.subject,
          subtitle: row.indicator_sub_title || '',
          unit: row.unit || '',
          target: row.target,
          direction: inferKpiDirection(row),
          min: row.min,
          max: row.max,
          low_limit: row.low_limit,
          high_limit: row.high_limit,
          tolerance_type: row.tolerance_type,
          up_tolerance: row.up_tolerance,
          low_tolerance: row.low_tolerance,
          frequency: row.frequency,
          definition: row.definition,
          _monthlyMap: {},   // { "Feb 2026": { sum, count } }
          comments: [],
          correctiveActions: []
        };
      }

      const value = parseFloat(row.new_value);
      if (!isNaN(value) && value > 0) {
        const month = weekToMonthLabel(row.week); // e.g. "Feb 2026"
        const mm = kpisData[kpiId]._monthlyMap;
        if (!mm[month]) mm[month] = { sum: 0, count: 0 };
        mm[month].sum += value;
        mm[month].count += 1;
      }

      if (row.comment && row.comment.trim())
        kpisData[kpiId].comments.push({ week: row.week, text: row.comment, date: row.updated_at });

      if (row.root_cause || row.implemented_solution || row.evidence) {
        const existingCA = kpisData[kpiId].correctiveActions.find(ca => ca.week === row.week);
        if (!existingCA) {
          kpisData[kpiId].correctiveActions.push({
            week: row.week,
            root_cause: row.root_cause,
            implemented_solution: row.implemented_solution,
            evidence: row.evidence,
            status: row.ca_status || 'Open',
            created_date: row.ca_created_date,
            updated_date: row.ca_updated_date
          });
        }
      }
    });

    // ── Pass 2: convert monthly maps → sorted month labels + averaged values ─
    const monthLabelsSet = new Set();
    Object.values(kpisData).forEach(kpi => {
      const mm = kpi._monthlyMap;
      const sortedMonths = Object.keys(mm).sort((a, b) => new Date(a) - new Date(b));
      kpi.weeklyData = new Map();
      sortedMonths.forEach(m => {
        kpi.weeklyData.set(m, mm[m].sum / mm[m].count);
        monthLabelsSet.add(m);
      });
      delete kpi._monthlyMap;
    });

    // ── Sorted month labels for all KPIs ──────────────────────────────────────
    const weekLabels = Array.from(monthLabelsSet).sort((a, b) => new Date(a) - new Date(b));
    if (weekLabels.length === 0) return null;

    // ── Build chart objects ───────────────────────────────────────────────────
    const charts = [];
    for (const [kpiId, kpiData] of Object.entries(kpisData)) {
      const dataPoints = weekLabels.map(w => kpiData.weeklyData.get(w) || 0);
      if (!dataPoints.some(v => v > 0)) continue;

      let currentWeek = null, currentValue = 0, previousValue = 0;
      for (let i = weekLabels.length - 1; i >= 0; i--) {
        if (dataPoints[i] > 0) {
          currentWeek = weekLabels[i]; currentValue = dataPoints[i];
          for (let j = i - 1; j >= 0; j--) { if (dataPoints[j] > 0) { previousValue = dataPoints[j]; break; } }
          break;
        }
      }
      if (!currentWeek) continue;

      const nonZero = dataPoints.filter(v => v > 0);
      const avg = nonZero.reduce((s, v) => s + v, 0) / nonZero.length;
      let trend = '0.0%';
      if (previousValue > 0 && currentValue > 0) {
        const tv = ((currentValue - previousValue) / previousValue) * 100;
        trend = (tv >= 0 ? '+' : '') + tv.toFixed(1) + '%';
      }

      // Labels are already "Jan 2026", "Feb 2026" etc — pass through as-is
      const displayWeekLabels = weekLabels;

      charts.push({
        kpiId,
        title: kpiData.title,
        subtitle: kpiData.subtitle,
        unit: kpiData.unit,
        target: kpiData.target,
        direction: kpiData.direction,
        min: kpiData.min,
        max: kpiData.max,
        low_limit: kpiData.low_limit,
        high_limit: kpiData.high_limit,
        tolerance_type: kpiData.tolerance_type,
        up_tolerance: kpiData.up_tolerance,
        low_tolerance: kpiData.low_tolerance,
        frequency: kpiData.frequency,
        definition: kpiData.definition,
        data: dataPoints,
        weekLabels: displayWeekLabels,
        fullWeeks: weekLabels,
        currentWeek,
        comments: kpiData.comments.sort((a, b) => new Date(b.date) - new Date(a.date)),
        correctiveActions: kpiData.correctiveActions.sort((a, b) =>
          new Date(b.updated_date || b.created_date) - new Date(a.updated_date || a.created_date)
        ),
        stats: {
          current: currentValue.toFixed(kpiData.unit === '%' ? 1 : 2),
          previous: previousValue > 0 ? previousValue.toFixed(kpiData.unit === '%' ? 1 : 2) : 'N/A',
          average: avg.toFixed(kpiData.unit === '%' ? 1 : 2),
          trend,
          dataPoints: nonZero.length,
          totalWeeks: weekLabels.length
        }
      });
    }

    console.log(`Generated ${charts.length} KPI charts for responsible ${responsibleId}`);
    return charts;
  } catch (error) {
    console.error('Error generating weekly report data:', error);
    return null;
  }
};

function getCurrentWeek() {
  const now = new Date();
  const year = now.getFullYear();
  const startDate = new Date(year, 0, 1);
  const days = Math.floor((now - startDate) / (24 * 60 * 60 * 1000));
  const weekNumber = Math.ceil((days + startDate.getDay() + 1) / 7);
  return `${year}-Week${weekNumber}`;
}

const generateWeeklyReportEmail = async (responsibleId, reportWeek) => {
  try {
    const resResp = await pool.query(
      `SELECT r.responsible_id, r.name, r.email, r.plant_id, r.department_id,
              p.name AS plant_name, d.name AS department_name
       FROM public."Responsible" r
       JOIN public."Plant" p ON r.plant_id = p.plant_id
       JOIN public."Department" d ON r.department_id = d.department_id
       WHERE r.responsible_id = $1`, [responsibleId]
    );
    const responsible = resResp.rows[0];
    if (!responsible) throw new Error(`Responsible ${responsibleId} not found`);

    // ── Build charts using CURRENT (old) target from Kpi table ───────────────
    const chartsData = await generateWeeklyReportData(responsibleId, reportWeek);
    let chartsHtml = '';

    if (chartsData && chartsData.length > 0) {
      chartsData.forEach(chart => { chartsHtml += generateVerticalBarChart(chart); });
    } else {
      chartsHtml = `
        <div style="text-align:center;padding:60px;background:#f8f9fa;border-radius:12px;">
          <div style="font-size:48px;color:#adb5bd;margin-bottom:20px;">📊</div>
          <p style="color:#495057;margin:0;font-size:18px;">No KPI Data Available</p>
        </div>`;
    }

    const emailHtml = `<!DOCTYPE html><html><head><meta charset="utf-8"></head>
    <body style="margin:0;padding:0;font-family:Arial,sans-serif;background:#f4f6f9;">
      <table border="0" cellpadding="0" cellspacing="0" width="100%" style="background:#f4f6f9;">
        <tr><td align="center" style="padding:20px;">
          <table border="0" cellpadding="0" cellspacing="0" width="100%">
            <tr><td style="background:#0078D7;padding:30px;text-align:center;border-radius:8px 8px 0 0;">
              <h1 style="margin:0;color:white;font-size:24px;">📊 KPI Performance Report</h1>
              <p style="margin:10px 0 20px;color:rgba(255,255,255,0.9);">
                ${reportWeek.replace('2026-Week', 'Week ')} | ${responsible.name} | ${responsible.plant_name}
              </p>
              <table border="0" cellpadding="0" cellspacing="0" align="center"><tr>
                <td style="padding:0 8px;">
                  <a href="https://kpi-codir.azurewebsites.net/kpi-trends?responsible_id=${responsible.responsible_id}"
                     style="display:inline-block;padding:12px 24px;background:#38bdf8;color:white;
                            text-decoration:none;border-radius:8px;font-weight:600;font-size:14px;">
                    📈 View KPI Graphics</a>
                </td>
                <td style="padding:0 8px;">
                  <a href="https://kpi-codir.azurewebsites.net/dashboard?responsible_id=${responsible.responsible_id}"
                     style="display:inline-block;padding:12px 24px;background:#38bdf8;color:white;
                            text-decoration:none;border-radius:8px;font-weight:600;font-size:14px;">
                    📊 View Dashboard</a>
                </td>
              </tr></table>
            </td></tr>

            <tr><td style="padding:20px 30px 0;">
              <div style="background:#fff8e1;border:1px solid #ffe082;border-radius:8px;padding:14px 18px;">
                <span style="font-size:14px;color:#5f4200;">
                  📎 <strong>AI Recommendations PDF is attached</strong> — open it for root-cause analysis,
                  action plans and improvement roadmaps for each KPI.
                </span>
              </div>
            </td></tr>

            <tr><td style="padding:30px;">${chartsHtml}</td></tr>

            <tr><td style="padding:20px;background:#f8f9fa;border-top:1px solid #e9ecef;
                            text-align:center;font-size:12px;color:#666;">
              AVOCarbon KPI System • Generated ${new Date().toLocaleDateString('en-GB')}
            </td></tr>
          </table>
        </td></tr>
      </table>
    </body></html>`;

    // ── Generate PDF attachment ───────────────────────────────────────────────
    let pdfAttachment = null;
    try {
      console.log(`📄 Generating recommendations PDF for ${responsible.name}…`);
      const pdfBuffer = await generateKPIRecommendationsPDFBuffer(pool, responsibleId, reportWeek);
      if (pdfBuffer) {
        const weekLabel = reportWeek.replace('2026-Week', 'Week_');
        pdfAttachment = {
          filename: `KPI_Recommendations_${responsible.name.replace(/ /g, '_')}_${weekLabel}.pdf`,
          content: pdfBuffer,
          contentType: 'application/pdf',
        };
        console.log(`📄 PDF ready — ${(pdfBuffer.length / 1024).toFixed(1)} KB`);
      }
    } catch (pdfErr) {
      console.error(`⚠️ PDF generation failed for ${responsible.name}:`, pdfErr.message);
    }

    // ── SEND EMAIL (with OLD target values in charts) ─────────────────────────
    const transporter = createTransporter();
    await transporter.sendMail({
      from: '"AVOCarbon KPI System" <administration.STS@avocarbon.com>',
      to: responsible.email,
      subject: `📊 KPI Performance Trends - ${reportWeek} | ${responsible.name}`,
      html: emailHtml,
      attachments: pdfAttachment ? [pdfAttachment] : [],
    });
    console.log(`✅ Email sent to ${responsible.email}`);

    // ── NOW apply all pending target updates for this responsible ─────────────
    // Email is already sent — safe to update Kpi.target and hist26.target
    try {
      const pending = await pool.query(
        `SELECT p.id, p.kpi_id, p.week, p.new_target,
                k.target AS current_kpi_target, k.subject
         FROM public.pending_target_updates p
         JOIN public."Kpi" k ON k.kpi_id = p.kpi_id
         WHERE p.responsible_id = $1 AND p.applied = false`,
        [responsibleId]
      );

      console.log(`📋 ${pending.rows.length} pending target update(s) to apply for ${responsible.name}`);

      for (const row of pending.rows) {
        const newVal = parseFloat(row.new_target);
        const currVal = parseFloat(row.current_kpi_target);

        if (isNaN(newVal)) {
          console.warn(`⚠️ Skipping KPI ${row.kpi_id} — new_target "${row.new_target}" is not a number`);
          continue;
        }

        // 1. Update Kpi.target
        await pool.query(
          `UPDATE public."Kpi" SET target = $1 WHERE kpi_id = $2`,
          [String(newVal), row.kpi_id]
        );
        console.log(`🎯 Kpi.target updated: "${row.subject}" (${row.kpi_id}) ${currVal} → ${newVal}`);

        // 2. Update kpi_values_hist26.target for that week
        await pool.query(
          `UPDATE public.kpi_values_hist26
           SET target = $1
           WHERE responsible_id = $2 AND kpi_id = $3 AND week = $4`,
          [newVal, responsibleId, row.kpi_id, row.week]
        );
        console.log(`📝 kpi_values_hist26.target updated: KPI ${row.kpi_id} week ${row.week} → ${newVal}`);

        // 3. Mark as applied
        await pool.query(
          `UPDATE public.pending_target_updates SET applied = true WHERE id = $1`,
          [row.id]
        );
      }

      console.log(`✅ All pending target updates applied for ${responsible.name}`);

    } catch (applyErr) {
      console.error(`❌ Failed to apply pending target updates for ${responsible.name}:`, applyErr.message);
    }

  } catch (error) {
    console.error(`❌ generateWeeklyReportEmail failed for responsible ${responsibleId}:`, error.message);
    throw error;
  }
};
// ---------- Cron: weekly KPI submission email ----------
let cronRunning = false;
cron.schedule("53 09 * * *", async () => {
  const lockId = "send_kpi_weekly_email_job";
  const lock = await acquireJobLock(lockId);
  if (!lock.acquired) return;
  try {
    if (cronRunning) return;
    cronRunning = true;
    const now = new Date();
    const startOfYear = new Date(now.getFullYear(), 0, 1);
    const dayOfYear = Math.floor((now - startOfYear) / (24 * 60 * 60 * 1000));
    const currentWeek = Math.ceil((dayOfYear + startOfYear.getDay() + 1) / 7);
    const forcedWeek = `${now.getFullYear()}-Week${currentWeek}`;
    const resps = await pool.query(
      `SELECT DISTINCT r.responsible_id FROM public."Responsible" r
       JOIN public.kpi_values kv ON kv.responsible_id = r.responsible_id WHERE kv.week = $1`,
      [forcedWeek]
    );
    for (let r of resps.rows) await sendKPIEmail(r.responsible_id, forcedWeek);
    console.log(`✅ KPI emails sent to ${resps.rows.length} responsibles`);
  } catch (err) {
    console.error("❌ Scheduled email error:", err.message);
  } finally {
    cronRunning = false;
    await releaseJobLock(lockId, lock.instanceId, lock.lockHash);
  }
}, { scheduled: true, timezone: "Africa/Tunis" });


// ---------- Cron: weekly reports ----------
let reportCronRunning = false;
cron.schedule("17 11 * * *", async () => {
  const lockId = "weekly_kpi_report_job";
  const lock = await acquireJobLock(lockId);
  if (!lock.acquired) return;
  try {
    if (reportCronRunning) return;
    reportCronRunning = true;
    const now = new Date();
    const year = now.getFullYear();
    const getWeekNumber = (date) => {
      const d = new Date(date); d.setHours(0, 0, 0, 0);
      d.setDate(d.getDate() + 4 - (d.getDay() || 7));
      const yearStart = new Date(d.getFullYear(), 0, 1);
      return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
    };
    const weekNumber = getWeekNumber(now);
    const previousWeek = `${year}-Week${weekNumber - 1}`;
    const resps = await pool.query(
      `SELECT DISTINCT r.responsible_id, r.email, r.name
       FROM public."Responsible" r JOIN public.kpi_values_hist26 h ON r.responsible_id = h.responsible_id
       WHERE r.email IS NOT NULL AND r.email != ''
       GROUP BY r.responsible_id, r.email, r.name HAVING COUNT(h.hist_id) > 0`
    );
    for (const [index, resp] of resps.rows.entries()) {
      try {
        await generateWeeklyReportEmail(resp.responsible_id, previousWeek);
        await new Promise(resolve => setTimeout(resolve, 1500));
      } catch (err) {
        console.error(`Failed for ${resp.name}:`, err.message);
      }
    }
    console.log(`✅ Weekly reports sent`);
  } catch (error) {
    console.error("❌ Report cron error:", error.message);
  } finally {
    reportCronRunning = false;
    await releaseJobLock(lockId, lock.instanceId, lock.lockHash);
  }
}, { scheduled: true, timezone: "Africa/Tunis" });

// ============================================================
// createIndividualKPIChart
// ============================================================
const createIndividualKPIChart = (kpi) => {
  const target = kpi.target && kpi.target !== 'None' ? Number(kpi.target) : null;
  const high_limit = kpi.high_limit && kpi.high_limit !== 'None' ? Number(kpi.high_limit) : null;
  const low_limit = kpi.low_limit && kpi.low_limit !== 'None' ? Number(kpi.low_limit) : null;
  const direction = inferKpiDirection(kpi);

  const weeklyData = kpi.weeklyData || { weeks: [], values: [] };
  const weeks = weeklyData.weeks.slice(0, 12);
  const values = weeklyData.values.slice(0, 12);

  if (!values || values.length === 0 || values.every(v => v <= 0)) {
    return `<table border="0" cellpadding="15" cellspacing="0" width="100%"
              style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;margin-bottom:15px;">
      <tr><td style="text-align:center;color:#999;font-size:14px;padding:20px;">
        <div style="font-size:32px;opacity:0.3;">📊</div>
        <div>No data for ${kpi.subtitle || kpi.title}</div>
      </td></tr></table>`;
  }

  const fmt = (num) => {
    if (num === null || num === undefined || isNaN(num)) return '0';
    const w = Math.abs(num - Math.round(num)) < 0.0001;
    if (num >= 1e6) return (num / 1e6).toFixed(w ? 0 : 1) + 'M';
    if (num >= 1e3) return (num / 1e3).toFixed(w ? 0 : 1) + 'k';
    if (w) return Math.round(num).toString();
    if (num < 1) return num.toFixed(2);
    return num.toFixed(1);
  };
  const avg = values.reduce((a, b) => a + b, 0) / values.length;
  const currentValue = values[values.length - 1];

  const currentStatus = getKpiStatus(currentValue, low_limit, high_limit, direction);
  const trendColor = currentStatus.isGood === false ? '#dc2626' : '#22c55e';

  const statusArrow = currentStatus.isGood === false
    ? (direction === 'down' ? '↗' : '↘')
    : (direction === 'down' ? '↘' : '↗');



  const pointColors = values.map((v) =>
    getKpiStatus(v, low_limit, high_limit, direction).isGood === false
      ? '#dc2626'
      : '#22c55e'
  );

  const weekLabels = weeks.map(w =>
    typeof w === 'string' && w.includes('-Week')
      ? `W${w.split('-Week')[1]}`
      : w   // already a month label like "Feb 2026"
  );


  const datasets = [
    {
      label: (kpi.subtitle || kpi.title || '').substring(0, 40),
      data: values,
      borderColor: '#94a3b8',
      borderWidth: 2,
      lineTension: 0,
      pointBackgroundColor: pointColors,
      pointBorderColor: '#ffffff',
      pointBorderWidth: 2,
      pointRadius: 7,
      fill: false
    }
  ];

  if (high_limit !== null) {
    datasets.push({
      label: `High Limit (${fmt(high_limit)})`,
      data: new Array(values.length).fill(high_limit),
      borderColor: '#f97316', borderWidth: 2, borderDash: [6, 4],
      lineTension: 0, pointRadius: 0, fill: false
    });
  }

  if (low_limit !== null) {
    datasets.push({
      label: `Low Limit (${fmt(low_limit)})`,
      data: new Array(values.length).fill(low_limit),
      borderColor: '#dc2626', borderWidth: 2, borderDash: [6, 4],
      lineTension: 0, pointRadius: 0, fill: false
    });
  }

  const chartConfig = {
    type: 'line',
    data: { labels: weekLabels, datasets },
    options: {
      legend: { display: false },
      scales: {
        xAxes: [{ ticks: { fontSize: 10 }, gridLines: { color: 'rgba(0,0,0,0.05)' } }],
        yAxes: [{ ticks: { fontSize: 10, beginAtZero: false }, gridLines: { color: 'rgba(0,0,0,0.05)' } }]
      }
    }
  };

  const chartUrl =
    `https://quickchart.io/chart?c=${encodeURIComponent(JSON.stringify(chartConfig))}` +
    `&w=320&h=170&bkg=white`;

  const badges = `
    <table border="0" cellpadding="5" cellspacing="0" width="100%" style="margin-top:10px;">
      <tr>
        <td align="center" width="33%">${target !== null
      ? `<div style="background:#e8f5e9;color:#2e7d32;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #a5d6a7;display:inline-block;">🎯 Target: ${target}</div>`
      : `<div style="background:#f5f5f5;color:#9e9e9e;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #e0e0e0;display:inline-block;">🎯 N/A</div>`}</td>
        <td align="center" width="33%">${high_limit !== null
      ? `<div style="background:#fff3e0;color:#e65100;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #ffb74d;display:inline-block;">🔺 High Limit: ${high_limit}</div>`
      : `<div style="background:#f5f5f5;color:#9e9e9e;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #e0e0e0;display:inline-block;">🔺 N/A</div>`}</td>
        <td align="center" width="33%">${low_limit !== null
      ? `<div style="background:#ffebee;color:#c62828;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #ef5350;display:inline-block;">🔻 Low Limit: ${low_limit}</div>`
      : `<div style="background:#f5f5f5;color:#9e9e9e;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #e0e0e0;display:inline-block;">🔻 N/A</div>`}</td>
      </tr>
    </table>`;

  const hasComments = kpi.comments && kpi.comments.length > 0;
  const hasCA = kpi.correctiveAction && (
    kpi.correctiveAction.rootCause ||
    kpi.correctiveAction.implementedSolution ||
    kpi.correctiveAction.evidence
  );
  const hasSide = hasComments || hasCA;
  const leftWidth = hasSide ? '52%' : '100%';
  const rightWidth = hasSide ? '48%' : '0%';

  const caStatusBadge = hasCA && kpi.correctiveAction.status ? `
    <span style="display:inline-block;font-size:9px;font-weight:700;padding:2px 8px;
                 border-radius:10px;margin-left:6px;
                 background:${kpi.correctiveAction.status === 'Waiting for validation' ? '#fff3e0' : '#ffebee'};
                 color:${kpi.correctiveAction.status === 'Waiting for validation' ? '#e65100' : '#c62828'};
                 border:1px solid ${kpi.correctiveAction.status === 'Waiting for validation' ? '#ffcc80' : '#ef9a9a'};">
      ${kpi.correctiveAction.status}
    </span>` : '';

  const rightColumn = hasSide ? `
    <td width="${rightWidth}" valign="top"
        style="padding:18px 16px 18px 20px;
               border-left:3px solid #e2e8f0;
               background:#f8fafc;
               border-radius:0 8px 8px 0;">

      ${hasComments ? `
      <div style="margin-bottom:${hasCA ? '14px' : '0'};">
        <div style="font-size:11px;font-weight:700;color:#0078D7;
                    margin-bottom:10px;padding-bottom:6px;
                    border-bottom:1px solid #bfdbfe;letter-spacing:0.4px;">
          💬 COMMENTS
        </div>
        ${kpi.comments.map(c => `
          <table border="0" cellpadding="10" cellspacing="0" width="100%"
                 style="background:#eff6ff;border-radius:8px;
                        border-left:4px solid #3b82f6;margin-bottom:8px;">
            <tr><td style="font-size:10px;font-weight:800;color:#1d4ed8;
                           text-transform:uppercase;letter-spacing:0.5px;
                           padding-bottom:4px;">
              ${weekToMonthLabel(c.week || '') || (c.week || '').replace('2026-Week', 'Week ')}
            </td></tr>
            <tr><td style="font-size:12px;color:#1e3a5f;line-height:1.5;">${c.text}</td></tr>
          </table>`).join('')}
      </div>` : ''}

      ${hasCA ? `
      <div>
        <div style="font-size:11px;font-weight:700;color:#c62828;
                    margin-bottom:10px;padding-bottom:6px;
                    border-bottom:1px solid #fecaca;letter-spacing:0.4px;
                    display:flex;align-items:center;">
          ⚠️ CORRECTIVE ACTION ${caStatusBadge}
        </div>

        ${kpi.correctiveAction.rootCause ? `
        <table border="0" cellpadding="0" cellspacing="0" width="100%"
               style="margin-bottom:8px;">
          <tr><td style="padding:8px 10px;background:#fff5f5;border-radius:6px;
                         border-left:3px solid #ef4444;">
            <div style="font-size:9px;font-weight:800;color:#dc2626;
                        text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">
              🔍 Root Cause
            </div>
            <div style="font-size:11px;color:#374151;line-height:1.5;">
              ${kpi.correctiveAction.rootCause}
            </div>
          </td></tr>
        </table>` : ''}

        ${kpi.correctiveAction.implementedSolution ? `
        <table border="0" cellpadding="0" cellspacing="0" width="100%"
               style="margin-bottom:8px;">
          <tr><td style="padding:8px 10px;background:#fffbeb;border-radius:6px;
                         border-left:3px solid #f59e0b;">
            <div style="font-size:9px;font-weight:800;color:#d97706;
                        text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">
              ⚡ Implemented Solution
            </div>
            <div style="font-size:11px;color:#374151;line-height:1.5;">
              ${kpi.correctiveAction.implementedSolution}
            </div>
          </td></tr>
        </table>` : ''}

        ${kpi.correctiveAction.evidence ? `
        <table border="0" cellpadding="0" cellspacing="0" width="100%">
          <tr><td style="padding:8px 10px;background:#f0f9ff;border-radius:6px;
                         border-left:3px solid #3b82f6;">
            <div style="font-size:9px;font-weight:800;color:#2563eb;
                        text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">
              📊 Evidence
            </div>
            <div style="font-size:11px;color:#374151;line-height:1.5;">
              ${kpi.correctiveAction.evidence}
            </div>
          </td></tr>
        </table>` : ''}
      </div>` : ''}

    </td>` : '';

  return `
<table border="0" cellpadding="0" cellspacing="0" width="100%"
       style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;
              margin-bottom:15px;box-sizing:border-box;">
  <tr><td style="padding:18px 20px;">
    <table border="0" cellpadding="0" cellspacing="0" width="100%" style="margin-bottom:8px;">
      <tr>
        <td style="font-weight:700;font-size:13px;color:#333;padding-right:10px;">
          ${kpi.subtitle || kpi.title}
        </td>
        <td align="center" valign="middle" style="padding:0 8px;">
          <div style="background:#f1f5f9;border:1px solid #e2e8f0;padding:4px 10px;
                      border-radius:8px;display:inline-block;">
            <span style="font-size:11px;color:#475569;font-weight:600;">${kpi.responsible || 'N/A'}</span>
          </div>
        </td>
        <td align="right" width="36">
          <div style="background:${trendColor};color:#fff;padding:4px 8px;border-radius:6px;
                      font-weight:700;font-size:12px;display:inline-block;">${statusArrow}</div>
        </td>
      </tr>
    </table>
    <table border="0" cellpadding="0" cellspacing="0" width="100%" style="margin-bottom:14px;">
      <tr><td align="center">
        <span style="background:#8b5cf6;color:white;padding:5px 12px;border-radius:20px;
                     font-size:12px;font-weight:700;display:inline-block;">
          Current: ${currentValue.toFixed(kpi.unit === '%' ? 1 : 2)} ${kpi.unit || ''}
          &nbsp;|&nbsp;
          Avg: ${avg.toFixed(kpi.unit === '%' ? 1 : 2)} ${kpi.unit || ''}
        </span>
      </td></tr>
    </table>
    <table border="0" cellpadding="0" cellspacing="0" width="100%">
      <tr>
        <td width="${leftWidth}" valign="top"
            style="padding-right:${hasSide ? '20px' : '0'};">
          ${badges}
          <table border="0" cellpadding="0" cellspacing="0" width="100%" style="margin-top:10px;">
            <tr><td align="center">
              <img src="${chartUrl}" width="320" height="170" alt="KPI trend chart"
                   style="display:block;width:100%;max-width:320px;
                          border:1px solid #f0f0f0;border-radius:6px;" />
            </td></tr>
          </table>
          <table border="0" cellpadding="3" cellspacing="0" width="100%"
                 style="margin-top:8px;border-top:1px solid #f0f0f0;">
            <tr><td align="center">
              <table border="0" cellpadding="0" cellspacing="6"><tr>
                <td><table border="0" cellpadding="0" cellspacing="3"><tr>
                  <td width="10" height="10" style="background:#22c55e;border-radius:50%;font-size:0;">&nbsp;</td>
                  <td style="font-size:10px;color:#666;">Good direction</td>
                </tr></table></td>
                <td><table border="0" cellpadding="0" cellspacing="3"><tr>
                  <td width="10" height="10" style="background:#dc2626;border-radius:50%;font-size:0;">&nbsp;</td>
                  <td style="font-size:10px;color:#666;">Outside good direction</td>
                </tr></table></td>
              </tr></table>
            </td></tr>
          </table>
        </td>
        ${rightColumn}
      </tr>
    </table>
  </td></tr>
</table>`;
};

// ============================================================
// generateManagerReportHtml
// ============================================================
const generateManagerReportHtml = (reportData) => {
  const { plant, week, kpisByDepartment, stats } = reportData;
  let kpiSections = '';
  const departments = Object.keys(kpisByDepartment).sort();
  departments.forEach(department => {
    const kpis = kpisByDepartment[department];
    if (kpis.length === 0) return;
    const color = getDepartmentColor(department);
    function createKPIRows(kpis) {
      let rows = '';
      for (let i = 0; i < kpis.length; i += 2) {
        const rowKPIs = kpis.slice(i, i + 2);
        rows += '<tr>';
        rowKPIs.forEach(kpi => { rows += `<td width="50%" valign="top" style="padding:12px;">${createIndividualKPIChart(kpi)}</td>`; });
        const empty = 2 - rowKPIs.length;
        for (let j = 0; j < empty; j++) rows += '<td width="50%" style="padding:12px;"></td>';
        rows += '</tr>';
      }
      return rows;
    }
    kpiSections += `
      <table border="0" cellpadding="0" cellspacing="0" width="100%" style="margin-bottom:40px;">
        <tr><td>
          <table border="0" cellpadding="0" cellspacing="0" width="100%"
                 style="margin-bottom:20px;padding-bottom:10px;border-bottom:3px solid ${color};">
            <tr><td style="padding:5px 0;">
              <span style="font-size:20px;font-weight:700;color:#2c3e50;
                           text-transform:uppercase;">${department}</span>
              <span style="font-size:12px;color:#6c757d;background:#f8f9fa;
                           padding:5px 14px;border-radius:12px;margin-left:10px;">${kpis.length} KPIs</span>
            </td></tr>
          </table>
          <table border="0" cellpadding="10" cellspacing="0" width="100%">
            ${createKPIRows(kpis)}
          </table>
        </td></tr>
      </table>`;
  });
  return `<!DOCTYPE html><html>
<head><meta charset="utf-8"><title>Plant Weekly KPI Dashboard</title>
<style>body{margin:0;padding:0;font-family:Arial,sans-serif;background:#f8f9fa;}</style></head>
<body>
  <div style="padding:30px 20px;max-width:1400px;margin:0 auto;">
    <div style="background:white;border-radius:12px;padding:30px;box-shadow:0 4px 12px rgba(0,0,0,0.05);margin-bottom:30px;">
      <h1 style="margin:0 0 8px;font-size:28px;font-weight:800;color:#2c3e50;">📊 CEO KPI CODIR DASHBOARD</h1>
      <div style="font-size:14px;color:#6c757d;">
        <strong>${plant.plant_name}</strong> • Week: <strong>${week.replace('2026-Week', 'W')}</strong>
        • Manager: <strong>${plant.manager || 'N/A'}</strong></div>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:15px;
                  background:#f8f9fa;padding:20px;border-radius:8px;margin-top:20px;">
        <div style="text-align:center;">
          <div style="font-size:28px;font-weight:800;color:#0078D7;">${stats.totalDepartments}</div>
          <div style="font-size:12px;color:#6c757d;">Departments</div></div>
        <div style="text-align:center;">
          <div style="font-size:28px;font-weight:800;color:#28a745;">${stats.totalKPIs}</div>
          <div style="font-size:12px;color:#6c757d;">Total KPIs</div></div>
        <div style="text-align:center;">
          <div style="font-size:28px;font-weight:800;color:#6f42c1;">${week.replace('2026-Week', 'W')}</div>
          <div style="font-size:12px;color:#6c757d;">Current Week</div></div>
      </div>
    </div>
    <div style="background:white;padding:30px;border-radius:12px;box-shadow:0 4px 12px rgba(0,0,0,0.05);">
      ${kpiSections || '<div style="text-align:center;padding:60px;color:#6c757d;">No KPI data available</div>'}
    </div>
    <div style="background:#2c3e50;color:white;padding:25px;border-radius:12px;text-align:center;margin-top:30px;">
      <div style="font-size:16px;font-weight:600;margin-bottom:10px;">AVOCarbon Plant Analytics</div>
      <div style="font-size:13px;opacity:0.8;">Weekly KPI Performance Monitoring</div>
    </div>
  </div>
</body></html>`;
};

const getDepartmentColor = (departmentName) => {
  if (!departmentName) return '#6c757d';
  if (departmentName.includes('Sales')) return '#667eea';
  if (departmentName.includes('Production')) return '#4facfe';
  if (departmentName.includes('Quality')) return '#43e97b';
  if (departmentName.includes('VOH')) return '#909d6f';
  if (departmentName.includes('Engineering')) return '#36a07b';
  if (departmentName.includes('Human resources')) return '#78d69a';
  if (departmentName.includes('Stocks')) return '#6a772a';
  if (departmentName.includes('AR/AP')) return '#96ce25';
  if (departmentName.includes('Cash')) return '#54591b';
  const colorMap = {
    'Production': '#667eea', 'Quality': '#f093fb', 'Maintenance': '#4facfe',
    'Safety': '#43e97b', 'Operations': '#fa709a', 'Engineering': '#30cfd0',
    'Supply-chain': '#f6d365', 'Administration': '#a8edea', 'Finance': '#f093fb',
    'HR': '#4facfe', 'IT': '#667eea', 'Sales': '#43e97b', 'Other': '#6c757d'
  };
  return colorMap[departmentName] || '#6c757d';
};

// ---------- KPI Trends page ----------
app.get("/kpi-trends", async (req, res) => {
  try {
    const { responsible_id } = req.query;
    const resResp = await pool.query(
      `SELECT r.*, p.name AS plant_name, d.name AS department_name
       FROM public."Responsible" r JOIN public."Plant" p ON r.plant_id = p.plant_id
       JOIN public."Department" d ON r.department_id = d.department_id
       WHERE r.responsible_id = $1`, [responsible_id]);
    const responsible = resResp.rows[0];
    if (!responsible) throw new Error("Responsible not found");

    const historyRes = await pool.query(
      `WITH KPIHistory AS (
         SELECT h.kpi_id, h.week, h.new_value, h.updated_at, h.comment,
                k.subject, k.indicator_sub_title, k.unit, k.target, k.min, k.max,
                k.tolerance_type, k.up_tolerance, k.low_tolerance, k.frequency, k.definition,
                k.calculation_on, k.high_limit, k.low_limit,
                ROW_NUMBER() OVER (PARTITION BY h.kpi_id, h.week ORDER BY h.updated_at DESC) as rn
         FROM public.kpi_values_hist26 h JOIN public."Kpi" k ON h.kpi_id = k.kpi_id
         WHERE h.responsible_id = $1
           AND h.new_value IS NOT NULL AND h.new_value != ''
           AND h.new_value ~ '^[0-9.]+$'
       )
       SELECT * FROM KPIHistory WHERE rn = 1
       ORDER BY kpi_id, CAST(SPLIT_PART(week, 'Week', 2) AS INTEGER)`,
      [responsible_id]
    );

    if (!historyRes.rows.length) {
      return res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"></head>
        <body style="font-family:'Inter',sans-serif;background:linear-gradient(135deg,#667eea,#764ba2);
          min-height:100vh;display:flex;justify-content:center;align-items:center;padding:20px;margin:0;">
          <div style="background:rgba(255,255,255,0.95);border-radius:24px;padding:60px 40px;
                      text-align:center;max-width:500px;">
            <div style="font-size:72px;margin-bottom:30px;">📊</div>
            <h1 style="color:#2c3e50;font-size:28px;">No KPI Trends Available</h1>
            <p style="color:#666;font-size:16px;">Start filling KPI forms to see trend charts.</p>
            <a href="/form?responsible_id=${responsible_id}&week=${getCurrentWeek()}"
               style="display:inline-block;margin-top:20px;padding:15px 30px;
                      background:linear-gradient(135deg,#667eea,#764ba2);color:white;
                      text-decoration:none;border-radius:12px;font-weight:600;">
              ✏️ Start Filling KPIs</a>
          </div>
        </body></html>`);
    }

    const kpiData = processKPIChartData(historyRes.rows);
    const html = generateTrendsDashboardHTML(responsible, kpiData);
    res.send(html);
  } catch (err) {
    console.error("KPI Trends error:", err);
    res.status(500).send(createErrorHTML(err.message));
  }
});

function processKPIChartData(rows) {
  const kpis = new Map();
  const weeksSet = new Set();

  rows.forEach(row => {
    const kpiId = row.kpi_id;
    if (!kpis.has(kpiId)) {
      kpis.set(kpiId, {
        id: kpiId, subject: row.subject, subtitle: row.indicator_sub_title,
        unit: row.unit || '',
        target: row.target && row.target !== 'None' ? parseFloat(row.target) : null,
        direction: inferKpiDirection(row),
        min: row.min && row.min !== 'None' ? parseFloat(row.min) : null,
        max: row.max && row.max !== 'None' ? parseFloat(row.max) : null,
        high_limit: row.high_limit && row.high_limit !== 'None' ? parseFloat(row.high_limit) : null,
        low_limit: row.low_limit && row.low_limit !== 'None' ? parseFloat(row.low_limit) : null,
        definition: row.definition, tolerance_type: row.tolerance_type,
        up_tolerance: row.up_tolerance, low_tolerance: row.low_tolerance,
        values: [], weeks: [], colors: [], comments: []
      });
    }
    const value = parseFloat(row.new_value);
    const kpi = kpis.get(kpiId);
    kpi.values.push(value);
    kpi.weeks.push(row.week);
    if (row.comment && row.comment.trim()) kpi.comments.push({ week: row.week, text: row.comment, date: row.updated_at });
    kpi.colors.push(getDotColor(value, kpi.low_limit, kpi.high_limit, kpi.direction));
    weeksSet.add(row.week);
  });

  const allWeeks = Array.from(weeksSet).sort((a, b) => {
    const [ya, wa] = a.includes('Week') ? [parseInt(a.split('-Week')[0]), parseInt(a.split('-Week')[1])] : [0, parseInt(a.replace('Week', ''))];
    const [yb, wb] = b.includes('Week') ? [parseInt(b.split('-Week')[0]), parseInt(b.split('-Week')[1])] : [0, parseInt(b.replace('Week', ''))];
    return ya !== yb ? ya - yb : wa - wb;
  });

  for (const [, kpi] of kpis) {
    const vals = kpi.values.filter(v => !isNaN(v) && v > 0);
    if (vals.length === 0) continue;
    kpi.average = vals.reduce((a, b) => a + b, 0) / vals.length;
    kpi.maxValue = Math.max(...vals);
    kpi.minValue = Math.min(...vals);
    if (vals.length >= 2) {
      const cur = vals[vals.length - 1], prev = vals[vals.length - 2];
      kpi.trend = ((cur - prev) / prev) * 100;
      kpi.trendIcon = cur > prev ? '↗' : cur < prev ? '↘' : '→';
      kpi.trendColor = cur > prev ? '#10b981' : cur < prev ? '#ef4444' : '#6b7280';
    } else { kpi.trend = 0; kpi.trendIcon = '→'; kpi.trendColor = '#6b7280'; }
    if (kpi.low_limit !== null || kpi.high_limit !== null) {
      const latest = vals[vals.length - 1];
      kpi.achievementVsLimit = !needsCorrectiveAction(latest, kpi.low_limit, kpi.high_limit, kpi.direction);
      kpi.achievementColor = getDotColor(latest, kpi.low_limit, kpi.high_limit, kpi.direction);
    }
  }

  return { kpis: Array.from(kpis.values()), allWeeks, totalKPIs: kpis.size, totalWeeks: allWeeks.length };
}

function generateTrendsDashboardHTML(responsible, kpiData) {
  const { kpis, allWeeks, totalKPIs, totalWeeks } = kpiData;
  return `<!DOCTYPE html><html>
<head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>KPI Trends - ${responsible.name}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    *{margin:0;padding:0;box-sizing:border-box;}
    body{font-family:'Segoe UI',sans-serif;background:linear-gradient(rgba(0,0,0,0.7),rgba(0,0,0,0.7)),
      url('https://images.unsplash.com/photo-1542744095-fcf48d80b0fd?w=1920') center/cover fixed;
      color:#fff;min-height:100vh;}
    .container{max-width:1400px;margin:0 auto;padding:20px;}
    .dashboard-header{background:rgba(31,41,55,0.95);border:1px solid rgba(255,255,255,0.1);
      backdrop-filter:blur(10px);border-radius:24px;padding:40px;margin-bottom:30px;}
    .stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:20px;margin-top:30px;}
    .stat-card{background:rgba(255,255,255,0.1);border-radius:16px;padding:20px;border:1px solid rgba(255,255,255,0.2);}
    .stat-value{font-size:32px;font-weight:700;margin-bottom:5px;}
    .stat-label{font-size:14px;opacity:0.8;text-transform:uppercase;letter-spacing:0.5px;}
    .kpi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(600px,1fr));gap:30px;margin-bottom:40px;}
    @media(max-width:1300px){.kpi-grid{grid-template-columns:1fr;}}
    .kpi-card{background:rgba(255,255,255,0.95);color:#1f2937;border-radius:20px;padding:30px;
      border:1px solid rgba(255,255,255,0.2);transition:all 0.3s ease;}
    .kpi-card:hover{transform:translateY(-5px);box-shadow:0 20px 40px rgba(0,0,0,0.3);}
    .kpi-header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:25px;}
    .chart-container{height:300px;margin:25px 0;position:relative;}
    .stats-row{display:grid;grid-template-columns:repeat(4,1fr);gap:15px;margin-top:25px;}
    .stat-box{background:#f9fafb;border-radius:12px;padding:15px;text-align:center;border:1px solid #e5e7eb;}
    .stat-box .label{font-size:12px;color:#6b7280;margin-bottom:5px;text-transform:uppercase;}
    .stat-box .value{font-size:20px;font-weight:700;color:#111827;}
    .footer{text-align:center;margin-top:50px;padding:30px;border-top:1px solid rgba(255,255,255,0.1);}
    @media(max-width:768px){.kpi-grid{grid-template-columns:1fr;}.stats-row{grid-template-columns:repeat(2,1fr);}}
  </style>
</head>
<body>
  <div class="container">
    <header class="dashboard-header">
      <h1 style="font-size:36px;font-weight:800;color:#fff;margin-bottom:10px;">📊 KPI Trends & Analytics</h1>
      <p style="font-size:18px;opacity:0.9;margin-bottom:25px;">Performance metrics across all production weeks</p>
      <div style="display:flex;gap:30px;flex-wrap:wrap;">
        <div><div style="font-size:12px;opacity:0.6;margin-bottom:5px;">OPERATOR</div>
          <div style="font-size:18px;font-weight:600;">${responsible.name}</div></div>
        <div><div style="font-size:12px;opacity:0.6;margin-bottom:5px;">FACTORY</div>
          <div style="font-size:18px;font-weight:600;">${responsible.plant_name}</div></div>
        <div><div style="font-size:12px;opacity:0.6;margin-bottom:5px;">DEPARTMENT</div>
          <div style="font-size:18px;font-weight:600;">${responsible.department_name}</div></div>
      </div>
      <div class="stats-grid">
        <div class="stat-card"><div class="stat-value" style="color:#60a5fa;">${totalKPIs}</div>
          <div class="stat-label">Active KPIs</div></div>
        <div class="stat-card"><div class="stat-value" style="color:#34d399;">${totalWeeks}</div>
          <div class="stat-label">Production Weeks</div></div>
        <div class="stat-card">
          <div class="stat-value" style="color:#fbbf24;">${kpis.reduce((acc, k) => acc + k.values.length, 0)}</div>
          <div class="stat-label">Data Points</div></div>
        <div class="stat-card">
          <div class="stat-value" style="color:#f87171;">${kpis.filter(k => k.low_limit !== null).length}</div>
          <div class="stat-label">KPIs with Limits</div></div>
      </div>
    </header>
    <main>
      <div class="kpi-grid">
        ${kpis.map((kpi, index) => generateKPIChartHTML(kpi, index)).join('')}
      </div>
    </main>
    <footer class="footer">
      <p style="color:rgba(255,255,255,0.8);">AVOCarbon Industrial Analytics • ${new Date().getFullYear()}</p>
    </footer>
  </div>
  <script>
    document.addEventListener('DOMContentLoaded', function() {
      ${kpis.map((kpi, index) => initializeChartJS(kpi, index)).join('\n')}
    });
  </script>
</body></html>`;
}

function generateKPIChartHTML(kpi, index) {
  const chartId = `chart-${index}`;
  return `
    <div class="kpi-card">
      <div class="kpi-header">
        <div>
          <h3 style="font-size:20px;font-weight:700;color:#111827;margin-bottom:5px;">${kpi.subject}</h3>
          ${kpi.subtitle ? `<p style="font-size:14px;color:#6b7280;">${kpi.subtitle}</p>` : ''}
        </div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;">
          ${kpi.unit ? `<span style="padding:6px 12px;border-radius:20px;font-size:12px;
            font-weight:600;background:#f3f4f6;color:#374151;">📏 ${kpi.unit}</span>` : ''}
          <span style="padding:6px 12px;border-radius:20px;font-size:12px;font-weight:600;
            background:${kpi.trendColor}20;color:${kpi.trendColor};">
            ${kpi.trendIcon} ${Math.abs(kpi.trend).toFixed(1)}%</span>
        </div>
      </div>
      <div class="chart-container"><canvas id="${chartId}"></canvas></div>
      
      <!-- Three key metrics side by side - Target, High Limit, Low Limit -->
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:15px;margin:25px 0;">
        <!-- Target Box -->
        <div style="background:#f9fafb;border-radius:12px;padding:15px;text-align:center;border:1px solid #e5e7eb;">
          <div style="font-size:12px;color:#6b7280;margin-bottom:5px;text-transform:uppercase;display:flex;align-items:center;justify-content:center;gap:4px;">
            <span>🎯</span> Target
          </div>
          <div style="font-size:24px;font-weight:700;color:#f97316;">
            ${kpi.target !== null ? kpi.target.toFixed(2) : 'N/A'}
          </div>
        </div>
        
        <!-- High Limit Box -->
        <div style="background:#f9fafb;border-radius:12px;padding:15px;text-align:center;border:1px solid #e5e7eb;">
          <div style="font-size:12px;color:#6b7280;margin-bottom:5px;text-transform:uppercase;display:flex;align-items:center;justify-content:center;gap:4px;">
            <span>🔺</span> High Limit
          </div>
          <div style="font-size:24px;font-weight:700;color:#f97316;">
            ${kpi.high_limit !== null ? kpi.high_limit.toFixed(2) : 'N/A'}
          </div>
        </div>
        
        <!-- Low Limit Box -->
        <div style="background:#f9fafb;border-radius:12px;padding:15px;text-align:center;border:1px solid #e5e7eb;">
          <div style="font-size:12px;color:#6b7280;margin-bottom:5px;text-transform:uppercase;display:flex;align-items:center;justify-content:center;gap:4px;">
            <span>🔻</span> Low Limit
          </div>
          <div style="font-size:24px;font-weight:700;color:#ef4444;">
            ${kpi.low_limit !== null ? kpi.low_limit.toFixed(2) : 'N/A'}
          </div>
        </div>
      </div>
      
      <!-- Secondary stats row (Average and Trend) -->
      <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:15px;margin-bottom:25px;">
        <div class="stat-box">
          <div class="label">Average</div>
          <div class="value" style="color:#3b82f6;">${kpi.average ? kpi.average.toFixed(2) : 'N/A'}</div>
        </div>
        <div class="stat-box">
          <div class="label">Trend</div>
          <div class="value" style="color:${kpi.trendColor};">${kpi.trendIcon} ${Math.abs(kpi.trend).toFixed(1)}%</div>
        </div>
      </div>
      
      <div style="display:flex;gap:15px;margin-top:15px;flex-wrap:wrap;">
        <div style="display:flex;align-items:center;gap:6px;">
          <div style="width:12px;height:12px;border-radius:50%;background:#22c55e;"></div>
          <span style="font-size:12px;color:#6b7280;">Within good direction</span></div>
        <div style="display:flex;align-items:center;gap:6px;">
          <div style="width:12px;height:12px;border-radius:50%;background:#ef4444;"></div>
          <span style="font-size:12px;color:#6b7280;">Outside good direction</span></div>
      </div>
      ${kpi.definition ? `
        <div style="margin-top:20px;padding:15px;background:#f8fafc;border-radius:12px;border-left:4px solid #667eea;">
          <div style="font-size:12px;color:#64748b;margin-bottom:5px;">ℹ️ Definition</div>
          <div style="font-size:14px;color:#475569;">${kpi.definition}</div>
        </div>` : ''}
    </div>`;
}

function initializeChartJS(kpi, index) {
  const chartId = `chart-${index}`;
  const weekLabels = kpi.weeks.map(w =>
    w.includes('Week') ? `W${w.split('-Week')[1] || w.replace('Week', '')}` : w
  );
  const lowLimit = kpi.low_limit != null ? parseFloat(kpi.low_limit) : null;
  const highLimit = kpi.high_limit != null ? parseFloat(kpi.high_limit) : null;
  const direction = kpi.direction === 'down' ? 'down' : 'up';

  const pointColors = JSON.stringify(kpi.values.map((v) => {
    const status = getKpiStatus(v, lowLimit, highLimit, direction);
    if (status.isGood === null) return '#6b7280';
    return status.isGood ? '#22c55e' : '#ef4444';
  }));

  const extraDatasets = [];
  if (highLimit !== null) {
    extraDatasets.push(`{
      label: 'High Limit',
      data: Array(${kpi.values.length}).fill(${highLimit}),
      type: 'line', borderColor: '#f97316',
      borderDash: [6,4], borderWidth: 2,
      pointRadius: 0, fill: false, tension: 0, order: 1
    }`);
  }
  if (lowLimit !== null) {
    extraDatasets.push(`{
      label: 'Low Limit',
      data: Array(${kpi.values.length}).fill(${lowLimit}),
      type: 'line', borderColor: '#ef4444',
      borderDash: [6,4], borderWidth: 2,
      pointRadius: 0, fill: false, tension: 0, order: 1
    }`);
  }

  return `(function(){
    const ctx${index} = document.getElementById('${chartId}').getContext('2d');
    const pc${index}  = ${pointColors};
    new Chart(ctx${index}, {
      type: 'line',
      data: {
        labels: ${JSON.stringify(weekLabels)},
        datasets: [
          {
            label: '${kpi.subject}',
            data: ${JSON.stringify(kpi.values)},
            borderColor: '#94a3b8',
            borderWidth: 2,
            backgroundColor: pc${index},
            pointBackgroundColor: pc${index},
            pointBorderColor: '#ffffff',
            pointBorderWidth: 2,
            pointRadius: 9,
            pointHoverRadius: 12,
            showLine: true,
            tension: 0,
            fill: false,
            order: 2
          }
          ${extraDatasets.length ? ',' + extraDatasets.join(',') : ''}
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top', labels: { font: { size: 12 }, padding: 20, usePointStyle: true } },
          tooltip: {
            backgroundColor: 'rgba(31,41,55,0.9)',
            padding: 12, cornerRadius: 8,
            callbacks: {
              label: function(ctx) {
                if (ctx.dataset.label === 'High Limit') return 'High Limit: ' + ctx.parsed.y;
                if (ctx.dataset.label === 'Low Limit')  return 'Low Limit: '  + ctx.parsed.y;
                return '${kpi.unit ? kpi.unit + ': ' : ''}' + ctx.parsed.y.toFixed(2);
              }
            }
          }
        },
        scales: {
          x: { grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { font: { size: 11 } } },
          y: { beginAtZero: false, grid: { color: 'rgba(0,0,0,0.05)' },
               ticks: { font: { size: 11 },
                 callback: function(v){ return v + '${kpi.unit ? ' ' + kpi.unit : ''}'; } } }
        },
        interaction: { intersect: false, mode: 'index' }
      }
    });
  })();`;
}

function createErrorHTML(message) {
  return `<!DOCTYPE html><html><head><meta charset="utf-8"></head>
    <body style="font-family:'Segoe UI',sans-serif;background:linear-gradient(135deg,#ef4444,#dc2626);
      min-height:100vh;display:flex;justify-content:center;align-items:center;padding:20px;margin:0;">
      <div style="background:rgba(255,255,255,0.95);border-radius:24px;padding:50px 40px;text-align:center;max-width:500px;">
        <div style="font-size:72px;color:#ef4444;margin-bottom:30px;">❌</div>
        <h1 style="color:#1f2937;font-size:28px;">Error Loading KPI Trends</h1>
        <div style="color:#6b7280;padding:15px;background:#fef2f2;border-radius:12px;margin:20px 0;">${message}</div>
        <a href="/" style="display:inline-block;padding:15px 30px;background:linear-gradient(135deg,#ef4444,#dc2626);
          color:white;text-decoration:none;border-radius:12px;font-weight:600;">Return to Home</a>
      </div>
    </body></html>`;
}

const getPreviousWeek = (currentWeek) => {
  const [yearStr, weekStr] = currentWeek.split('-Week');
  let year = parseInt(yearStr), weekNumber = parseInt(weekStr) - 1;
  if (weekNumber < 1) { year--; weekNumber = 52; }
  return `${year}-Week${weekNumber}`;
};

const weekToMonthLabel = (weekStr) => {
  const match = weekStr.match(/(\d{4})-Week(\d+)/);
  if (!match) return weekStr;
  const date = new Date(parseInt(match[1]), 0, 1 + (parseInt(match[2]) - 1) * 7);
  return date.toLocaleString('en-US', { month: 'short', year: 'numeric' }); // "Feb 2026"
};


const getDepartmentKPIReport = async (plantId, week) => {
  try {
    const plantRes = await pool.query(
      `SELECT p.plant_id, p.name AS plant_name, p.manager, p.manager_email
       FROM public."Plant" p WHERE p.plant_id = $1 AND p.manager_email IS NOT NULL`,
      [plantId]
    );
    const plant = plantRes.rows[0];
    if (!plant || !plant.manager_email) return null;

    const kpiRes = await pool.query(
      `WITH LatestKPIValues AS (
         SELECT h.kpi_id, h.responsible_id, r.name AS responsible_name, h.week, h.new_value,
                h.updated_at, h.comment, r.department_id, d.name AS department_name,
                k.indicator_title, k.indicator_sub_title, k.unit, k.target, k.min, k.max,
                k.high_limit, k.low_limit,
                ROW_NUMBER() OVER (PARTITION BY h.kpi_id, h.responsible_id, h.week ORDER BY h.updated_at DESC) as rn
         FROM public.kpi_values_hist26 h
         JOIN public."Responsible" r ON h.responsible_id = r.responsible_id
         JOIN public."Department" d ON r.department_id = d.department_id
         JOIN public."Kpi" k ON h.kpi_id = k.kpi_id
         WHERE r.plant_id = $1 AND h.week = $2
           AND h.new_value IS NOT NULL AND h.new_value != ''
           AND CAST(h.new_value AS TEXT) ~ '^[0-9.]+$'
       )
       SELECT lkv.*,
              ca.root_cause, ca.implemented_solution, ca.evidence, ca.status AS ca_status
       FROM LatestKPIValues lkv
       LEFT JOIN LATERAL (
         SELECT root_cause, implemented_solution, evidence, status
         FROM public.corrective_actions
         WHERE kpi_id = lkv.kpi_id
           AND responsible_id = lkv.responsible_id
           AND week = lkv.week
         ORDER BY COALESCE(updated_date, created_date) DESC, corrective_action_id DESC
         LIMIT 1
       ) ca ON TRUE
       WHERE lkv.rn = 1 ORDER BY lkv.indicator_title`,
      [plantId, week]
    );

    if (!kpiRes.rows.length) return null;

    const weeklyTrendRes = await pool.query(
      `WITH WeeklyKPIData AS (
         SELECT k.kpi_id, k.indicator_title, k.indicator_sub_title, k.unit,
                k.target, k.min, k.max, k.high_limit, k.low_limit,
                h.week, AVG(CAST(h.new_value AS NUMERIC)) as avg_value,
                CAST(SPLIT_PART(h.week, 'Week', 2) AS INTEGER) as week_num
         FROM public.kpi_values_hist26 h
         JOIN public."Kpi" k ON h.kpi_id = k.kpi_id
         JOIN public."Responsible" r ON h.responsible_id = r.responsible_id
         WHERE r.plant_id = $1 AND h.new_value IS NOT NULL AND h.new_value != ''
           AND CAST(h.new_value AS TEXT) ~ '^[0-9.]+$' AND h.week LIKE '2026-Week%'
         GROUP BY k.kpi_id, k.indicator_title, k.indicator_sub_title, k.unit,
                  k.target, k.min, k.max, k.high_limit, k.low_limit, h.week
       )
       SELECT * FROM WeeklyKPIData ORDER BY kpi_id, week_num DESC LIMIT 500`,
      [plantId]
    );

    const extractDept = (t) => {
      if (!t) return 'Other';
      if (t.includes('Actual - ')) {
        const ex = t.split('Actual - ')[1];
        if (ex.includes('/')) return ex.split('/')[0].trim();
        if (ex.includes('(')) return ex.split('(')[0].trim();
        return ex.trim();
      }
      return t;
    };

    const kpisByDepartment = {};
    const weeklyDataByKPI = {};

    // Accumulate weekly values grouped into months
    weeklyTrendRes.rows.forEach(row => {
      const key = `${row.kpi_id}_${row.indicator_title}`;
      const dept = extractDept(row.indicator_title);
      const month = weekToMonthLabel(row.week); // e.g. "Feb 2026"

      if (!weeklyDataByKPI[key]) {
        weeklyDataByKPI[key] = {
          kpi_id: row.kpi_id, title: row.indicator_title, subtitle: row.indicator_sub_title || '',
          unit: row.unit || '', target: row.target, min: row.min, max: row.max,
          high_limit: row.high_limit, low_limit: row.low_limit,
          department: dept,
          _monthlyMap: {},  // { "Feb 2026": { sum, count } }
          weeks: [], values: []
        };
      }

      const mm = weeklyDataByKPI[key]._monthlyMap;
      if (!mm[month]) mm[month] = { sum: 0, count: 0 };
      mm[month].sum += parseFloat(row.avg_value);
      mm[month].count += 1;
    });

    // Convert monthly map → sorted arrays
    Object.values(weeklyDataByKPI).forEach(kpi => {
      const mm = kpi._monthlyMap || {};
      const sortedMonths = Object.keys(mm).sort((a, b) => new Date(a) - new Date(b));
      kpi.weeks = sortedMonths;                                        // ["Jan 2026", "Feb 2026", …]
      kpi.values = sortedMonths.map(m => mm[m].sum / mm[m].count);     // monthly average
      delete kpi._monthlyMap;
    });

    kpiRes.rows.forEach(row => {
      const dept = extractDept(row.indicator_title);
      const key = `${row.kpi_id}_${row.indicator_title}`;
      if (!kpisByDepartment[dept]) kpisByDepartment[dept] = [];
      let existing = kpisByDepartment[dept].find(k => k.id === row.kpi_id && k.title === row.indicator_title);
      if (!existing) {
        existing = {
          id: row.kpi_id, title: row.indicator_title, subtitle: row.indicator_sub_title || '',
          unit: row.unit || '', target: row.target, min: row.min, max: row.max,
          high_limit: row.high_limit, low_limit: row.low_limit,
          department: dept, originalDepartment: row.department_name,
          currentValue: parseFloat(row.new_value),
          weeklyData: weeklyDataByKPI[key] || { weeks: [], values: [] },
          lastUpdated: row.updated_at, responsible: row.responsible_name || '',
          comments: [], correctiveAction: null
        };
        kpisByDepartment[dept].push(existing);
      }
      if (row.comment && row.comment.trim())
        existing.comments.push({ week: row.week, text: row.comment.trim() });
      if (!existing.correctiveAction && (row.root_cause || row.implemented_solution || row.evidence)) {
        existing.correctiveAction = {
          rootCause: (row.root_cause || '').trim(),
          implementedSolution: (row.implemented_solution || '').trim(),
          evidence: (row.evidence || '').trim(),
          status: row.ca_status || ''
        };
      }
    });

    const sortedDepts = {};
    Object.keys(kpisByDepartment).sort().forEach(d => { sortedDepts[d] = kpisByDepartment[d]; });

    return {
      plant, week, kpisByDepartment: sortedDepts,
      stats: {
        totalDepartments: Object.keys(sortedDepts).length,
        totalKPIs: Object.values(sortedDepts).reduce((s, k) => s + k.length, 0),
        totalValues: kpiRes.rows.length
      }
    };
  } catch (error) {
    console.error(`Error getting KPI report for plant ${plantId}:`, error.message);
    return null;
  }
};

const sendDepartmentKPIReportEmail = async (plantId, currentWeek) => {
  try {
    const prevWeek = getPreviousWeek(currentWeek);
    const reportData = await getDepartmentKPIReport(plantId, prevWeek);
    if (!reportData || reportData.stats.totalKPIs === 0) return null;

    const emailHtml = generateManagerReportHtml(reportData);

    // ── Generate plant-wide recommendations PDF ───────────────────────────────
    let pdfAttachment = null;
    try {
      console.log(`📄 Generating plant-wide recommendations PDF for plant=${plantId}…`);
      const pdfBuffer = await generatePlantKPIRecommendationsPDFBuffer(pool, plantId, prevWeek);
      if (pdfBuffer) {
        const weekLabel = prevWeek.replace('2026-Week', 'Week_');
        pdfAttachment = {
          filename: `KPI_Recommendations_${reportData.plant.plant_name.replace(/ /g, '_')}_${weekLabel}.pdf`,
          content: pdfBuffer,
          contentType: 'application/pdf',
        };
        console.log(`📄 Plant PDF ready — ${(pdfBuffer.length / 1024).toFixed(1)} KB`);
      }
    } catch (pdfErr) {
      // Never block the main email if PDF generation fails
      console.error(`⚠️ Could not generate plant recommendations PDF:`, pdfErr.message);
    }

    // ── Send email with optional PDF attachment ───────────────────────────────
    const transporter = createTransporter();
    await transporter.sendMail({
      from: '"AVOCarbon Plant Analytics" <administration.STS@avocarbon.com>',
      to: reportData.plant.manager_email,
      subject: `📊 Weekly KPI Dashboard - ${reportData.plant.plant_name} - Week ${prevWeek.replace('2026-Week', '')}`,
      html: emailHtml,
      attachments: pdfAttachment ? [pdfAttachment] : [],
    });

    console.log(`✅ KPI report${pdfAttachment ? ' + recommendations PDF' : ''} sent to ${reportData.plant.manager_email}`);
  } catch (error) {
    console.error(`❌ Failed to send report for plant ${plantId}:`, error.message);
  }
};

// ---------- Cron: weekly manager/plant report ----------
let managerCronRunning = false;
cron.schedule("06 10 * * *", async () => {
  const lockId = "department_report_job";
  const lock = await acquireJobLock(lockId);
  if (!lock.acquired) return;
  try {
    if (managerCronRunning) return;
    managerCronRunning = true;
    const now = new Date();
    const year = now.getFullYear();
    const getWeekNumber = (date) => {
      const d = new Date(date); d.setHours(0, 0, 0, 0);
      d.setDate(d.getDate() + 4 - (d.getDay() || 7));
      const yearStart = new Date(d.getFullYear(), 0, 1);
      return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
    };
    const weekNumber = getWeekNumber(now);
    const currentWeek = `${year}-Week${weekNumber}`;
    console.log(`📊 [Manager Report] Sending reports for week ${currentWeek}...`);
    const plantsRes = await pool.query(
      `SELECT plant_id, name, manager_email FROM public."Plant"
       WHERE manager_email IS NOT NULL AND manager_email != ''`
    );
    console.log(`📋 Found ${plantsRes.rows.length} plants with manager emails`);
    for (const plant of plantsRes.rows) {
      try {
        await sendDepartmentKPIReportEmail(plant.plant_id, currentWeek);
        console.log(`  ✅ Report sent for plant: ${plant.name}`);
        await new Promise(resolve => setTimeout(resolve, 1500));
      } catch (err) {
        console.error(`  ❌ Failed for plant ${plant.name}:`, err.message);
      }
    }
    console.log(`✅ [Manager Report] All plant reports sent`);
  } catch (error) {
    console.error("❌ [Manager Report] Cron error:", error.message);
  } finally {
    managerCronRunning = false;
    await releaseJobLock(lockId, lock.instanceId, lock.lockHash);
  }
}, { scheduled: true, timezone: "Africa/Tunis" });


registerRecommendationRoutes(app, pool, createTransporter);
// ---------- Start server ----------
app.listen(port, () => console.log(`🚀 Server running on port ${port}`));
