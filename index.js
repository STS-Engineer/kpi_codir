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

// ---------- Postgres ----------
const sharedPostgresConfig = {
  user: "administrationSTS",
  host: "avo-adb-002.postgres.database.azure.com",
  password: "St$@0987",
  port: 5432,
  ssl: { rejectUnauthorized: false },
};

const pool = new Pool({
  ...sharedPostgresConfig,
  database: "KPI_Test",
});

const actionPlanPool = new Pool({
  ...sharedPostgresConfig,
  database: "Action Plan",
});

const NUMERIC_TEXT_PATTERN = /^[+-]?(?:\d+\.?\d*|\.\d+)$/;

const createHttpError = (statusCode, message) => {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
};

const normalizeOptionalTextInput = (value) => {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  return text === "" ? null : text;
};

const normalizeOptionalNumericInput = (value, { allowPercent = false } = {}) => {
  const text = normalizeOptionalTextInput(value);
  if (text === null) return null;

  let normalized = text.replace(/\s+/g, "").replace(/,/g, ".");
  if (allowPercent && normalized.endsWith("%")) {
    normalized = normalized.slice(0, -1);
  }

  return NUMERIC_TEXT_PATTERN.test(normalized) ? normalized : null;
};

const normalizeOptionalIntegerInput = (value) => {
  const text = normalizeOptionalTextInput(value);
  if (text === null || !/^\d+$/.test(text)) return null;
  return Number(text);
};

const getPeopleDisplayName = (row = {}) => {
  const firstName = normalizeOptionalTextInput(row.first_name);
  const name = normalizeOptionalTextInput(row.name);

  if (firstName && name && firstName !== name) {
    return `${firstName} ${name}`;
  }

  return name || firstName || null;
};

const prepareKpiWritePayload = (payload = {}) => {
  const rawSubjectNodeId = normalizeOptionalTextInput(payload.subject_node_id ?? payload.subject_id);
  const rawTarget = normalizeOptionalTextInput(payload.target ?? payload.target_value);
  const rawMinValue = normalizeOptionalTextInput(payload.min ?? payload.min_value);
  const rawMaxValue = normalizeOptionalTextInput(payload.max ?? payload.max_value);
  const rawUpTolerance = normalizeOptionalTextInput(payload.up_tolerance);
  const rawLowTolerance = normalizeOptionalTextInput(payload.low_tolerance);
  const rawHighLimit = normalizeOptionalTextInput(payload.high_limit);
  const rawLowLimit = normalizeOptionalTextInput(payload.low_limit);
  const rawReferenceKpiId = normalizeOptionalTextInput(payload.reference_kpi_id);
  const rawOwnerRoleId = normalizeOptionalTextInput(payload.owner_role_id);

  const subjectNodeId = rawSubjectNodeId === null ? null : Number(rawSubjectNodeId);
  const targetValue = normalizeOptionalNumericInput(rawTarget);
  const minValue = normalizeOptionalNumericInput(rawMinValue);
  const maxValue = normalizeOptionalNumericInput(rawMaxValue);
  const upTolerance = normalizeOptionalNumericInput(rawUpTolerance, { allowPercent: true });
  const lowTolerance = normalizeOptionalNumericInput(rawLowTolerance, { allowPercent: true });
  const highLimit = normalizeOptionalNumericInput(rawHighLimit);
  const lowLimit = normalizeOptionalNumericInput(rawLowLimit);
  const calculationMode = normalizeOptionalTextInput(payload.calculation_mode);
  const referenceKpiId = rawReferenceKpiId === null ? null : Number(rawReferenceKpiId);
  const ownerRoleId = rawOwnerRoleId === null ? null : Number(rawOwnerRoleId);
  const kpiName = normalizeOptionalTextInput(payload.kpi_name ?? payload.indicator_sub_title);
  const unit = normalizeOptionalTextInput(payload.unit);
  const explanation = normalizeOptionalTextInput(payload.kpi_explanation ?? payload.definition);
  const status = normalizeOptionalTextInput(payload.status) || "Active";

  if (!kpiName) {
    throw createHttpError(400, "KPI name is required.");
  }

  if (!unit) {
    throw createHttpError(400, "Unit is required.");
  }

  if (!explanation) {
    throw createHttpError(400, "Definition is required.");
  }

  if (rawSubjectNodeId !== null && (!Number.isInteger(subjectNodeId) || subjectNodeId <= 0)) {
    throw createHttpError(400, "Subject selection is invalid.");
  }

  if (rawTarget !== null && targetValue === null) {
    throw createHttpError(400, "Target must be numeric.");
  }

  if (rawMinValue !== null && minValue === null) {
    throw createHttpError(400, "Min value must be numeric.");
  }

  if (rawMaxValue !== null && maxValue === null) {
    throw createHttpError(400, "Max value must be numeric.");
  }

  if (rawUpTolerance !== null && upTolerance === null) {
    throw createHttpError(400, 'Up tolerance must be numeric. Use values like "100" or "100%".');
  }

  if (rawLowTolerance !== null && lowTolerance === null) {
    throw createHttpError(400, 'Low tolerance must be numeric. Use values like "60" or "60%".');
  }

  if (rawHighLimit !== null && highLimit === null) {
    throw createHttpError(400, "High limit must be numeric.");
  }

  if (rawLowLimit !== null && lowLimit === null) {
    throw createHttpError(400, "Low limit must be numeric.");
  }

  if (rawReferenceKpiId !== null && (!Number.isInteger(referenceKpiId) || referenceKpiId <= 0)) {
    throw createHttpError(400, "Reference KPI_ID must be a valid integer.");
  }

  if (String(calculationMode || "").toLowerCase() === "ratio" && referenceKpiId === null) {
    throw createHttpError(400, "Reference KPI_ID is required for Ratio mode.");
  }

  if (rawOwnerRoleId !== null && (!Number.isInteger(ownerRoleId) || ownerRoleId <= 0)) {
    throw createHttpError(400, "Owner role must be a valid integer.");
  }

  return {
    subject_id: subjectNodeId,
    indicator_title: normalizeOptionalTextInput(payload.indicator_title),
    indicator_sub_title: kpiName,
    kpi_name: kpiName,
    kpi_sub_title: normalizeOptionalTextInput(payload.kpi_sub_title ?? payload.indicator_title),
    kpi_code: normalizeOptionalTextInput(payload.kpi_code),
    kpi_formula: normalizeOptionalTextInput(payload.kpi_formula),
    unit,
    subject: normalizeOptionalTextInput(payload.subject),
    definition: explanation,
    kpi_explanation: explanation,
    frequency: normalizeOptionalTextInput(payload.frequency),
    target: targetValue === null ? null : Number(targetValue),
    target_value: targetValue === null ? null : Number(targetValue),
    target_direction: normalizeOptionalTextInput(payload.target_direction),
    tolerance_type: normalizeOptionalTextInput(payload.tolerance_type),
    up_tolerance: rawUpTolerance === null ? null : rawUpTolerance.replace(/\s+/g, ""),
    low_tolerance: rawLowTolerance === null ? null : rawLowTolerance.replace(/\s+/g, ""),
    max: maxValue === null ? null : Number(maxValue),
    max_value: maxValue === null ? null : Number(maxValue),
    min: minValue === null ? null : Number(minValue),
    min_value: minValue === null ? null : Number(minValue),
    target_direction: normalizeOptionalTextInput(payload.target_direction || payload.direction),
    nombre_periode: normalizeOptionalTextInput(payload.nombre_periode),
    calculation_mode: calculationMode,
    display_trend: normalizeOptionalTextInput(payload.display_trend),
    regression: normalizeOptionalTextInput(payload.regression),
    min_type: normalizeOptionalTextInput(payload.min_type),
    max_type: normalizeOptionalTextInput(payload.max_type),
    calculation_on: normalizeOptionalTextInput(payload.calculation_on),
    target_auto_adjustment: normalizeOptionalTextInput(payload.target_auto_adjustment),
    reference_kpi_id: String(calculationMode || "").toLowerCase() === "ratio" ? referenceKpiId : null,
    owner_role_id: ownerRoleId,
    status,
    comments: normalizeOptionalTextInput(payload.comments),
    high_limit: highLimit === null ? null : Number(highLimit),
    low_limit: lowLimit === null ? null : Number(lowLimit)
  };
};

const normalizeOptionalDateInput = (value) => {
  const text = normalizeOptionalTextInput(value);
  if (text === null) return null;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;

  const parsed = new Date(`${text}T00:00:00Z`);
  return Number.isNaN(parsed.getTime()) ? null : text;
};

const prepareResponsibleParameterKpiPayload = (payload = {}) => {
  const rawKpiId = normalizeOptionalTextInput(payload.kpi_id);
  const rawValue = normalizeOptionalTextInput(payload.value);
  const rawTarget = normalizeOptionalTextInput(payload.target);
  const rawBestNewTarget = normalizeOptionalTextInput(payload.best_new_target);
  const rawValueDate = normalizeOptionalTextInput(payload.value_date);
  const kpiId = rawKpiId === null ? null : Number(rawKpiId);

  const normalizedValue = normalizeOptionalNumericInput(rawValue);
  const normalizedTarget = normalizeOptionalNumericInput(rawTarget);
  const normalizedBestNewTarget = normalizeOptionalNumericInput(rawBestNewTarget);
  const normalizedValueDate = normalizeOptionalDateInput(rawValueDate);

  if (rawKpiId === null || !Number.isInteger(kpiId) || kpiId <= 0) {
    throw createHttpError(400, "KPI Name is required.");
  }

  if (rawValue !== null && normalizedValue === null) {
    throw createHttpError(400, "Value must be numeric.");
  }

  if (rawTarget !== null && normalizedTarget === null) {
    throw createHttpError(400, "Target must be numeric.");
  }

  if (rawBestNewTarget !== null && normalizedBestNewTarget === null) {
    throw createHttpError(400, "Best new target must be numeric.");
  }

  if (rawValueDate !== null && normalizedValueDate === null) {
    throw createHttpError(400, "Date must use YYYY-MM-DD format.");
  }

  const location = normalizeOptionalTextInput(payload.location);
  const localCurrency = normalizeOptionalTextInput(payload.local_currency);
  const kpiType = normalizeOptionalTextInput(payload.kpi_type);

  if (!location) {
    throw createHttpError(400, "Location is required.");
  }

  if (!localCurrency) {
    throw createHttpError(400, "Local currency is required.");
  }

  if (!kpiType) {
    throw createHttpError(400, "KPI type is required.");
  }

  return {
    kpi_id: kpiId,
    location,
    local_currency: localCurrency,
    value: normalizedValue === null ? null : Number(normalizedValue),
    value_date: normalizedValueDate,
    target: normalizedTarget === null ? null : Number(normalizedTarget),
    best_new_target: normalizedBestNewTarget === null ? null : Number(normalizedBestNewTarget),
    kpi_type: kpiType
  };
};

let responsibleParameterKpiSchemaPromise = null;
let kpiRatioSchemaPromise = null;
let kpiObjectSchemaPromise = null;

const ensureKpiRatioSchema = async () => {
  if (!kpiRatioSchemaPromise) {
    kpiRatioSchemaPromise = Promise.resolve().catch((error) => {
      kpiRatioSchemaPromise = null;
      throw error;
    });
  }

  return kpiRatioSchemaPromise;
};

const ensureResponsibleParameterKpiSchema = async () => {
  if (!responsibleParameterKpiSchemaPromise) {
    responsibleParameterKpiSchemaPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS public.responsible_parameter_kpis (
          parameter_kpi_id BIGSERIAL PRIMARY KEY,
          responsible_id INTEGER NOT NULL
            REFERENCES public."Responsible" (responsible_id)
            ON DELETE CASCADE,
          kpi_id INTEGER
            REFERENCES public."Kpi" (kpi_id)
            ON DELETE SET NULL,
          location VARCHAR(255) NOT NULL,
          local_currency VARCHAR(50) NOT NULL,
          value NUMERIC(18, 2),
          value_date DATE,
          target NUMERIC(18, 2),
          best_new_target NUMERIC(18, 2),
          kpi_type VARCHAR(120) NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);

      await pool.query(`
        ALTER TABLE public.responsible_parameter_kpis
        ADD COLUMN IF NOT EXISTS kpi_id INTEGER
      `);

      await pool.query(`
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'fk_responsible_parameter_kpis_kpi'
          ) THEN
            ALTER TABLE public.responsible_parameter_kpis
            ADD CONSTRAINT fk_responsible_parameter_kpis_kpi
            FOREIGN KEY (kpi_id)
            REFERENCES public."Kpi" (kpi_id)
            ON DELETE SET NULL;
          END IF;
        END
        $$;
      `);

      await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_responsible_parameter_kpis_responsible
        ON public.responsible_parameter_kpis (responsible_id, created_at DESC)
      `);

      await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_responsible_parameter_kpis_kpi
        ON public.responsible_parameter_kpis (kpi_id)
      `);
    })().catch((error) => {
      responsibleParameterKpiSchemaPromise = null;
      throw error;
    });
  }

  return responsibleParameterKpiSchemaPromise;
};

const ensureKpiObjectSchema = async () => {
  if (!kpiObjectSchemaPromise) {
    kpiObjectSchemaPromise = Promise.resolve().catch((error) => {
      kpiObjectSchemaPromise = null;
      throw error;
    });
  }

  return kpiObjectSchemaPromise;
};

const getResponsibleNameById = async (responsibleId) => {
  const normalizedId = normalizeOptionalIntegerInput(responsibleId);
  if (normalizedId === null) return null;

  const result = await pool.query(
    `
    SELECT name, first_name
    FROM public.people
    WHERE people_id = $1
    LIMIT 1
    `,
    [normalizedId]
  );

  return getPeopleDisplayName(result.rows[0]);
};

const loadPeopleContextByPeopleId = async (peopleId) => {
  const normalizedPeopleId = normalizeOptionalIntegerInput(peopleId);
  if (!normalizedPeopleId) return null;

  const peopleRes = await pool.query(
    `
    SELECT
      p.people_id,
      p.first_name,
      p.name,
      p.email,
      u.unit_name AS plant_name
    FROM public.people p
    LEFT JOIN public.unit u
      ON u.unit_id = p.work_at_unit_id
    WHERE p.people_id = $1
    LIMIT 1
    `,
    [normalizedPeopleId]
  );

  const row = peopleRes.rows[0];
  if (!row) return null;

  return {
    people_id: row.people_id,
    name: getPeopleDisplayName(row) || `People #${normalizedPeopleId}`,
    email: normalizeOptionalTextInput(row.email),
    plant_name: normalizeOptionalTextInput(row.plant_name) || "Allocated scope",
    department_name: ""
  };
};

const loadLegacyResponsibleIdentity = async (responsibleId) => {
  const normalizedResponsibleId = normalizeOptionalIntegerInput(responsibleId);
  if (!normalizedResponsibleId) return null;

  try {
    const result = await pool.query(
      `
      SELECT responsible_id, name, email
      FROM public."Responsible"
      WHERE responsible_id = $1
      LIMIT 1
      `,
      [normalizedResponsibleId]
    );

    const row = result.rows[0];
    if (!row) return null;

    return {
      responsible_id: row.responsible_id,
      name: normalizeOptionalTextInput(row.name),
      email: normalizeOptionalTextInput(row.email)
    };
  } catch (error) {
    return null;
  }
};

const findPeopleContextForLegacyResponsible = async (legacyResponsible = {}) => {
  const legacyEmail = normalizeOptionalTextInput(legacyResponsible.email);
  const legacyName = normalizeOptionalTextInput(legacyResponsible.name);
  if (!legacyEmail && !legacyName) return null;

  const result = await pool.query(
    `
    SELECT
      p.people_id,
      p.first_name,
      p.name,
      p.email,
      u.unit_name AS plant_name
    FROM public.people p
    LEFT JOIN public.unit u
      ON u.unit_id = p.work_at_unit_id
    WHERE
      ($1::text IS NOT NULL AND LOWER(COALESCE(p.email, '')) = LOWER($1))
      OR (
        $2::text IS NOT NULL
        AND (
          LOWER(TRIM(CONCAT_WS(' ', COALESCE(p.first_name, ''), COALESCE(p.name, '')))) = LOWER($2)
          OR LOWER(COALESCE(p.name, '')) = LOWER($2)
          OR LOWER(COALESCE(p.first_name, '')) = LOWER($2)
        )
      )
    ORDER BY
      CASE
        WHEN $1::text IS NOT NULL AND LOWER(COALESCE(p.email, '')) = LOWER($1) THEN 0
        WHEN $2::text IS NOT NULL
          AND LOWER(TRIM(CONCAT_WS(' ', COALESCE(p.first_name, ''), COALESCE(p.name, '')))) = LOWER($2) THEN 1
        WHEN $2::text IS NOT NULL AND LOWER(COALESCE(p.name, '')) = LOWER($2) THEN 2
        WHEN $2::text IS NOT NULL AND LOWER(COALESCE(p.first_name, '')) = LOWER($2) THEN 3
        ELSE 4
      END,
      p.people_id ASC
    LIMIT 1
    `,
    [legacyEmail, legacyName]
  );

  const row = result.rows[0];
  if (!row) return null;

  return {
    people_id: row.people_id,
    name: getPeopleDisplayName(row) || legacyName || `People #${row.people_id}`,
    email: normalizeOptionalTextInput(row.email) || legacyEmail,
    plant_name: normalizeOptionalTextInput(row.plant_name) || "Allocated scope",
    department_name: ""
  };
};


const prepareKpiObjectPayload = (payload = {}) => {
  const rawKpiId = normalizeOptionalTextInput(payload.kpi_id);
  const rawPlantId = normalizeOptionalTextInput(payload.plant_id);
  const rawZoneId = normalizeOptionalTextInput(payload.zone_id);
  const rawUnitId = normalizeOptionalTextInput(payload.unit_id);
  const rawProductId = normalizeOptionalTextInput(payload.product_id);
  const rawProductLineId = normalizeOptionalTextInput(payload.product_line_id);
  const rawMarketId = normalizeOptionalTextInput(payload.market_id);
  const rawCustomerId = normalizeOptionalTextInput(payload.customer_id);
  const rawSetByPeopleId = normalizeOptionalTextInput(payload.set_by_people_id);
  const rawApprovedByPeopleId = normalizeOptionalTextInput(payload.approved_by_people_id);
  const rawTargetValue = normalizeOptionalTextInput(payload.target_value ?? payload.target);
  const rawLastBestTarget = normalizeOptionalTextInput(payload.last_best_target ?? payload.best_new_target);
  const rawPreviousTargetValue = normalizeOptionalTextInput(payload.previous_target_value ?? payload.value);
  const rawTargetSetupDate = normalizeOptionalTextInput(payload.target_setup_date ?? payload.value_date);
  const rawTargetStartDate = normalizeOptionalTextInput(payload.target_start_date);
  const rawTargetEndDate = normalizeOptionalTextInput(payload.target_end_date);
  const kpiId = rawKpiId === null ? null : Number(rawKpiId);
  const plantId = rawPlantId === null ? null : Number(rawPlantId);
  const zoneId = rawZoneId === null ? null : Number(rawZoneId);
  const unitId = rawUnitId === null ? null : Number(rawUnitId);
  const productId = rawProductId === null ? null : Number(rawProductId);
  const productLineId = rawProductLineId === null ? null : Number(rawProductLineId);
  const marketId = rawMarketId === null ? null : Number(rawMarketId);
  const customerId = rawCustomerId === null ? null : Number(rawCustomerId);
  const setByPeopleId = rawSetByPeopleId === null ? null : Number(rawSetByPeopleId);
  const approvedByPeopleId = rawApprovedByPeopleId === null ? null : Number(rawApprovedByPeopleId);

  const normalizedTargetValue = normalizeOptionalNumericInput(rawTargetValue);
  const normalizedLastBestTarget = normalizeOptionalNumericInput(rawLastBestTarget);
  const normalizedPreviousTargetValue = normalizeOptionalNumericInput(rawPreviousTargetValue);
  const normalizedTargetSetupDate = normalizeOptionalDateInput(rawTargetSetupDate);
  const normalizedTargetStartDate = normalizeOptionalDateInput(rawTargetStartDate);
  const normalizedTargetEndDate = normalizeOptionalDateInput(rawTargetEndDate);

  if (rawKpiId === null || !Number.isInteger(kpiId) || kpiId <= 0) {
    throw createHttpError(400, "KPI Name is required.");
  }

  if (rawTargetValue === null || normalizedTargetValue === null) {
    throw createHttpError(400, "Target value is required and must be numeric.");
  }

  if (rawLastBestTarget !== null && normalizedLastBestTarget === null) {
    throw createHttpError(400, "Last best target must be numeric.");
  }

  if (rawPreviousTargetValue !== null && normalizedPreviousTargetValue === null) {
    throw createHttpError(400, "Previous target value must be numeric.");
  }

  if (rawTargetSetupDate === null || normalizedTargetSetupDate === null) {
    throw createHttpError(400, "Target setup date is required and must use YYYY-MM-DD format.");
  }

  if (rawTargetStartDate !== null && normalizedTargetStartDate === null) {
    throw createHttpError(400, "Target start date must use YYYY-MM-DD format.");
  }

  if (rawTargetEndDate !== null && normalizedTargetEndDate === null) {
    throw createHttpError(400, "Target end date must use YYYY-MM-DD format.");
  }

  if (
    normalizedTargetStartDate &&
    normalizedTargetEndDate &&
    normalizedTargetStartDate > normalizedTargetEndDate
  ) {
    throw createHttpError(400, "Target end date must be after the start date.");
  }

  const localCurrency = normalizeOptionalTextInput(payload.local_currency);
  const kpiType = normalizeOptionalTextInput(payload.kpi_type);
  const targetUnit = normalizeOptionalTextInput(payload.target_unit);
  const targetStatus = normalizeOptionalTextInput(payload.target_status) || "Draft";
  const functionName = normalizeOptionalTextInput(payload.function);

  for (const [label, rawValue, numericValue] of [
    ["Plant", rawPlantId, plantId],
    ["Zone", rawZoneId, zoneId],
    ["Unit", rawUnitId, unitId],
    ["Product", rawProductId, productId],
    ["Product line", rawProductLineId, productLineId],
    ["Market", rawMarketId, marketId],
    ["Customer", rawCustomerId, customerId],
    ["Set by people", rawSetByPeopleId, setByPeopleId],
    ["Approved by people", rawApprovedByPeopleId, approvedByPeopleId]
  ]) {
    if (rawValue !== null && (!Number.isInteger(numericValue) || numericValue <= 0)) {
      throw createHttpError(400, `${label} must be a valid selection.`);
    }
  }

  return {
    kpi_id: kpiId,
    local_currency: localCurrency,
    plant_id: plantId,
    zone_id: zoneId,
    unit_id: unitId,
    function: functionName,
    product_id: productId,
    product_line_id: productLineId,
    market_id: marketId,
    customer_id: customerId,
    target_value: Number(normalizedTargetValue),
    target_unit: targetUnit,
    kpi_type: kpiType,
    target_setup_date: normalizedTargetSetupDate,
    target_start_date: normalizedTargetStartDate,
    target_end_date: normalizedTargetEndDate,
    last_best_target: normalizedLastBestTarget === null ? null : Number(normalizedLastBestTarget),
    previous_target_value: normalizedPreviousTargetValue === null ? null : Number(normalizedPreviousTargetValue),
    target_status: targetStatus,
    set_by_people_id: setByPeopleId,
    approved_by_people_id: approvedByPeopleId,
    comments: normalizeOptionalTextInput(payload.comments)
  };
};

const loadSubjectHierarchyData = async () => {
  const result = await pool.query(`
    SELECT
      subject_id,
      parent_subject_id,
      subject_name,
      subject_explanation,
      subject_example,
      knowledge,
      status,
      comments
    FROM public.subject
    ORDER BY subject_id
  `);

  const nodeMap = new Map();

  result.rows.forEach((row) => {
    const id = String(row.subject_id);
    nodeMap.set(id, {
      id,
      name: normalizeOptionalTextInput(row.subject_name) || `Subject ${row.subject_id}`,
      parent_id: row.parent_subject_id === null ? null : String(row.parent_subject_id),
      parent_name: null,
      level: 0,
      full_path: normalizeOptionalTextInput(row.subject_name) || `Subject ${row.subject_id}`,
      description: normalizeOptionalTextInput(row.subject_explanation) || "",
      comment: normalizeOptionalTextInput(row.comments) || "",
      why_important: normalizeOptionalTextInput(row.knowledge) || "",
      subject_example: normalizeOptionalTextInput(row.subject_example) || "",
      status: normalizeOptionalTextInput(row.status) || "",
      children: []
    });
  });

  nodeMap.forEach((node) => {
    if (node.parent_id && nodeMap.has(node.parent_id)) {
      nodeMap.get(node.parent_id).children.push(node);
    }
  });

  const resolvePath = (node) => {
    if (!node) return "";
    if (node.__resolvedPath) return node.__resolvedPath;

    if (node.parent_id && nodeMap.has(node.parent_id)) {
      const parentNode = nodeMap.get(node.parent_id);
      const parentPath = resolvePath(parentNode);
      node.parent_name = parentNode.name || null;
      node.level = (parentNode.level || 0) + 1;
      node.__resolvedPath = `${parentPath} / ${node.name}`;
    } else {
      node.parent_name = null;
      node.level = 0;
      node.__resolvedPath = node.name;
    }

    node.full_path = node.__resolvedPath;
    return node.__resolvedPath;
  };

  const roots = [];
  const pathById = new Map();

  nodeMap.forEach((node) => {
    resolvePath(node);
    pathById.set(String(node.id), node.full_path);

    if (!node.parent_id || !nodeMap.has(node.parent_id)) {
      roots.push(node);
    }

    delete node.__resolvedPath;
  });

  const sortNodes = (nodes = []) => {
    nodes.sort((left, right) => String(left.name || "").localeCompare(String(right.name || "")));
    nodes.forEach((node) => sortNodes(node.children));
    return nodes;
  };

  return {
    tree: sortNodes(roots),
    nodeMap,
    pathById
  };
};

const loadRoleOptions = async () => {
  const result = await pool.query(`
    SELECT role_id, role_name, role_code
    FROM public.role
    ORDER BY role_name, role_id
  `);

  return result.rows.map((row) => ({
    value: String(row.role_id),
    label: [normalizeOptionalTextInput(row.role_name), normalizeOptionalTextInput(row.role_code)]
      .filter(Boolean)
      .join(" · ") || `Role ${row.role_id}`
  }));
};

const loadKpiTargetAllocationLookups = async () => {
  const [
    unitsResult,
    zonesResult,
    productLinesResult,
    productsResult,
    marketsResult,
    customersResult,
    peopleResult
  ] = await Promise.all([
    pool.query(`
      SELECT unit_id, unit_name, unit_type_id
      FROM public.unit
      ORDER BY unit_name, unit_id
    `),
    pool.query(`
      SELECT zone_id, zone_name
      FROM public.zone
      ORDER BY zone_name, zone_id
    `),
    pool.query(`
      SELECT product_line_id, product_line_name
      FROM public.product_line
      ORDER BY product_line_name, product_line_id
    `),
    pool.query(`
      SELECT product_id, product_name
      FROM public.product
      ORDER BY product_name, product_id
    `),
    pool.query(`
      SELECT market_id, market_name
      FROM public.market
      ORDER BY market_name, market_id
    `),
    pool.query(`
      SELECT customer_id, customer_name
      FROM public.customer
      ORDER BY customer_name, customer_id
    `),
    pool.query(`
      SELECT people_id, name, first_name
      FROM public.people
      ORDER BY name, first_name, people_id
    `)
  ]);

  const unitOptions = unitsResult.rows.map((row) => ({
    value: String(row.unit_id),
    label: normalizeOptionalTextInput(row.unit_name) || `Unit ${row.unit_id}`
  }));

  return {
    plants: unitOptions,
    units: unitOptions,
    zones: zonesResult.rows.map((row) => ({
      value: String(row.zone_id),
      label: normalizeOptionalTextInput(row.zone_name) || `Zone ${row.zone_id}`
    })),
    productLines: productLinesResult.rows.map((row) => ({
      value: String(row.product_line_id),
      label: normalizeOptionalTextInput(row.product_line_name) || `Product line ${row.product_line_id}`
    })),
    products: productsResult.rows.map((row) => ({
      value: String(row.product_id),
      label: normalizeOptionalTextInput(row.product_name) || `Product ${row.product_id}`
    })),
    markets: marketsResult.rows.map((row) => ({
      value: String(row.market_id),
      label: normalizeOptionalTextInput(row.market_name) || `Market ${row.market_id}`
    })),
    customers: customersResult.rows.map((row) => ({
      value: String(row.customer_id),
      label: normalizeOptionalTextInput(row.customer_name) || `Customer ${row.customer_id}`
    })),
    people: peopleResult.rows.map((row) => ({
      value: String(row.people_id),
      label: getPeopleDisplayName(row) || `Person ${row.people_id}`
    }))
  };
};

const getDashboardOwnerDisplayName = async (responsibleId) => {
  return (await loadFormResponsibleContext(responsibleId))?.name || "KPI Workspace";
};

const mapKpiRowToClient = (row, subjectPathById = new Map()) => {
  const subjectId = row.subject_id === null || row.subject_id === undefined
    ? null
    : String(row.subject_id);
  const subjectPath = subjectId ? subjectPathById.get(subjectId) || row.subject_name || "" : "";
  const setByPeopleDisplayName = getPeopleDisplayName({
    name: row.set_by_people_name,
    first_name: row.set_by_people_first_name
  }) || "";

  return {
    kpi_id: row.kpi_id,
    indicator_title: row.kpi_sub_title || "",
    indicator_sub_title: row.kpi_name || "",
    kpi_code: row.kpi_code || "",
    kpi_formula: row.kpi_formula || "",
    unit: row.unit || "",
    subject: subjectPath,
    subject_node_id: subjectId,
    definition: row.kpi_explanation || "",
    frequency: row.frequency || "",
    target: row.allocation_target_value ?? row.target_value,
    target_direction: row.target_direction || "",
    tolerance_type: row.tolerance_type || "",
    up_tolerance: row.up_tolerance || "",
    low_tolerance: row.low_tolerance || "",
    max: row.max_value,
    min: row.min_value,
    calculation_on: row.calculation_on || "",
    nombre_periode: row.nombre_periode || "",
    calculation_mode: row.calculation_mode || "",
    display_trend: row.display_trend || "",
    regression: row.regression || "",
    min_type: row.min_type || "",
    max_type: row.max_type || "",
    reference_kpi_id: row.reference_kpi_id,
    target_auto_adjustment: row.target_auto_adjustment || "",
    high_limit: row.high_limit,
    low_limit: row.low_limit,
    owner_role_id: row.owner_role_id,
    status: row.status || "Active",
    comments: row.comments || "",
    created_at: row.created_at,
    updated_at: row.updated_at,
    full_subject_path: subjectPath,
    kpi_target_allocation_id: row.kpi_target_allocation_id ?? null,
    allocation_target_value: row.allocation_target_value ?? null,
    allocation_target_unit: row.allocation_target_unit || "",
    allocation_target_status: row.allocation_target_status || "",
    set_by_people_id: row.set_by_people_id ?? null,
    set_by_people_display_name: setByPeopleDisplayName
  };
};

const loadKpiRowsForUi = async ({ search = "", kpiId = null, responsibleId = null } = {}) => {
  const subjectHierarchy = await loadSubjectHierarchyData();
  const searchText = normalizeOptionalTextInput(search) || "";
  const normalizedResponsibleId = normalizeOptionalIntegerInput(responsibleId);
  const params = [searchText, `%${searchText}%`, kpiId, normalizedResponsibleId];

  const result = await pool.query(
    `
    SELECT
      k.kpi_id,
      k.kpi_name,
      k.kpi_sub_title,
      k.kpi_code,
      k.kpi_formula,
      k.kpi_explanation,
      k.target_value,
      k.target_direction,
      k.target_auto_adjustment,
      k.min_value,
      k.min_type,
      k.max_value,
      k.max_type,
      k.low_limit,
      k.high_limit,
      k.tolerance_type,
      k.low_tolerance,
      k.up_tolerance,
      k.unit,
      k.frequency,
      k.nombre_periode,
      k.calculation_mode,
      k.calculation_on,
      k.display_trend,
      k.regression,
      k.reference_kpi_id,
      k.owner_role_id,
      k.status,
      k.comments,
      k.created_at,
      k.updated_at,
      primary_subject.subject_id,
      assignment.kpi_target_allocation_id,
      assignment.target_value AS allocation_target_value,
      assignment.target_unit AS allocation_target_unit,
      assignment.target_status AS allocation_target_status,
      assignment.set_by_people_id,
      set_by_people.name AS set_by_people_name,
      set_by_people.first_name AS set_by_people_first_name
    FROM public.kpi k
    ${normalizedResponsibleId === null ? "" : `
    JOIN LATERAL (
      SELECT
        a.kpi_target_allocation_id,
        a.target_value,
        a.target_unit,
        a.target_status,
        a.set_by_people_id
      FROM public.kpi_target_allocation a
      WHERE a.kpi_id = k.kpi_id
        AND a.set_by_people_id = $4
      ORDER BY COALESCE(a.updated_at, a.created_at) DESC, a.kpi_target_allocation_id DESC
      LIMIT 1
    ) assignment ON TRUE
    LEFT JOIN public.people set_by_people
      ON set_by_people.people_id = assignment.set_by_people_id
    `}
    ${normalizedResponsibleId === null ? `
    LEFT JOIN LATERAL (
      SELECT NULL::integer AS kpi_target_allocation_id,
             NULL::numeric AS target_value,
             NULL::varchar AS target_unit,
             NULL::varchar AS target_status,
             NULL::integer AS set_by_people_id
    ) assignment ON TRUE
    LEFT JOIN public.people set_by_people
      ON 1 = 0
    ` : ""}
    LEFT JOIN LATERAL (
      SELECT sk.subject_id
      FROM public.subject_kpi sk
      WHERE sk.kpi_id = k.kpi_id
      ORDER BY COALESCE(sk.is_primary, false) DESC, sk.subject_kpi_id ASC
      LIMIT 1
    ) primary_subject ON TRUE
    WHERE
      ($3::integer IS NULL OR k.kpi_id = $3)
      AND ($4::integer IS NULL OR assignment.kpi_target_allocation_id IS NOT NULL)
      AND (
        $1 = ''
        OR COALESCE(k.kpi_name, '') ILIKE $2
        OR COALESCE(k.kpi_sub_title, '') ILIKE $2
        OR COALESCE(k.kpi_code, '') ILIKE $2
        OR COALESCE(k.kpi_explanation, '') ILIKE $2
        OR COALESCE(k.frequency, '') ILIKE $2
        OR EXISTS (
          SELECT 1
          FROM public.subject_kpi sk2
          JOIN public.subject s2
            ON s2.subject_id = sk2.subject_id
          WHERE sk2.kpi_id = k.kpi_id
            AND COALESCE(s2.subject_name, '') ILIKE $2
        )
      )
    ORDER BY k.kpi_id DESC
    `,
    params
  );

  return result.rows.map((row) => mapKpiRowToClient(row, subjectHierarchy.pathById));
};

const resolveSubjectIdForKpiPayload = async (payload = {}) => {
  if (Number.isInteger(payload.subject_id) && payload.subject_id > 0) {
    return payload.subject_id;
  }

  const subjectText = normalizeOptionalTextInput(payload.subject);
  if (!subjectText) return null;

  const subjectHierarchy = await loadSubjectHierarchyData();
  const loweredSubjectText = subjectText.toLowerCase();

  for (const [subjectId, fullPath] of subjectHierarchy.pathById.entries()) {
    if (String(fullPath || "").toLowerCase() === loweredSubjectText) {
      return Number(subjectId);
    }
  }

  for (const [subjectId, node] of subjectHierarchy.nodeMap.entries()) {
    if (String(node?.name || "").toLowerCase() === loweredSubjectText) {
      return Number(subjectId);
    }
  }

  return null;
};

const upsertPrimarySubjectKpiLink = async (client, kpiId, subjectId) => {
  await client.query(
    `
    UPDATE public.subject_kpi
    SET is_primary = FALSE
    WHERE kpi_id = $1
    `,
    [kpiId]
  );

  const existing = await client.query(
    `
    SELECT subject_kpi_id
    FROM public.subject_kpi
    WHERE kpi_id = $1
      AND subject_id = $2
    ORDER BY subject_kpi_id ASC
    LIMIT 1
    `,
    [kpiId, subjectId]
  );

  if (existing.rows.length) {
    await client.query(
      `
      UPDATE public.subject_kpi
      SET
        is_primary = TRUE,
        importance_level = COALESCE(importance_level, 'Primary')
      WHERE subject_kpi_id = $1
      `,
      [existing.rows[0].subject_kpi_id]
    );
    return;
  }

  await client.query(
    `
    INSERT INTO public.subject_kpi (
      subject_id,
      kpi_id,
      importance_level,
      is_primary,
      comments
    )
    VALUES ($1, $2, $3, $4, $5)
    `,
    [subjectId, kpiId, "Primary", true, null]
  );
};

const loadKpiTargetAllocationRowsForUi = async ({ allocationId = null, responsibleId = null } = {}) => {
  const subjectHierarchy = await loadSubjectHierarchyData();
  const normalizedResponsibleId = normalizeOptionalIntegerInput(responsibleId);
  const result = await pool.query(
    `
    SELECT
      a.kpi_target_allocation_id,
      a.kpi_id,
      a.plant_id,
      plant.unit_name AS plant_name,
      a.zone_id,
      zone.zone_name,
      a.unit_id,
      unit_scope.unit_name AS unit_name,
      a.function,
      a.product_id,
      product.product_name,
      a.product_line_id,
      product_line.product_line_name,
      a.market_id,
      market.market_name,
      a.customer_id,
      customer.customer_name,
      a.target_value,
      a.target_unit,
      a.local_currency,
      a.kpi_type,
      a.target_setup_date,
      a.target_start_date,
      a.target_end_date,
      a.last_best_target,
      a.previous_target_value,
      a.target_status,
      a.set_by_people_id,
      set_by_people.name AS set_by_people_name,
      set_by_people.first_name AS set_by_people_first_name,
      a.approved_by_people_id,
      approved_by_people.name AS approved_by_people_name,
      approved_by_people.first_name AS approved_by_people_first_name,
      a.comments,
      a.created_at,
      a.updated_at,
      k.kpi_name,
      k.kpi_sub_title,
      primary_subject.subject_id AS subject_id
    FROM public.kpi_target_allocation a
    LEFT JOIN public.kpi k
      ON k.kpi_id = a.kpi_id
    LEFT JOIN public.unit plant
      ON plant.unit_id = a.plant_id
    LEFT JOIN public.zone zone
      ON zone.zone_id = a.zone_id
    LEFT JOIN public.unit unit_scope
      ON unit_scope.unit_id = a.unit_id
    LEFT JOIN public.product product
      ON product.product_id = a.product_id
    LEFT JOIN public.product_line product_line
      ON product_line.product_line_id = a.product_line_id
    LEFT JOIN public.market market
      ON market.market_id = a.market_id
    LEFT JOIN public.customer customer
      ON customer.customer_id = a.customer_id
    LEFT JOIN public.people set_by_people
      ON set_by_people.people_id = a.set_by_people_id
    LEFT JOIN public.people approved_by_people
      ON approved_by_people.people_id = a.approved_by_people_id
    LEFT JOIN LATERAL (
      SELECT sk.subject_id
      FROM public.subject_kpi sk
      WHERE sk.kpi_id = a.kpi_id
      ORDER BY COALESCE(sk.is_primary, false) DESC, sk.subject_kpi_id ASC
      LIMIT 1
    ) primary_subject ON TRUE
    WHERE ($1::integer IS NULL OR a.kpi_target_allocation_id = $1)
      AND ($2::integer IS NULL OR a.set_by_people_id = $2)
    ORDER BY a.created_at DESC, a.kpi_target_allocation_id DESC
    `,
    [allocationId, normalizedResponsibleId]
  );

  return result.rows.map((row) => ({
    ...row,
    kpi_subject: row.subject_id ? subjectHierarchy.pathById.get(String(row.subject_id)) || "" : "",
    kpi_group: row.kpi_sub_title || "",
    set_by_people_display_name: getPeopleDisplayName({
      name: row.set_by_people_name,
      first_name: row.set_by_people_first_name
    }) || "",
    approved_by_people_display_name: getPeopleDisplayName({
      name: row.approved_by_people_name,
      first_name: row.approved_by_people_first_name
    }) || ""
  }));
};

app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// ========== KPI ADMIN API ==========

// GET all KPIs
app.get("/api/kpis", async (req, res) => {
  try {
    await ensureKpiRatioSchema();
    const rows = await loadKpiRowsForUi({ search: req.query.search || "" });
    res.json(rows);
  } catch (err) {
    console.error("GET /api/kpis error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET one KPI
app.get("/api/kpis/:id", async (req, res) => {
  try {
    await ensureKpiRatioSchema();
    const rows = await loadKpiRowsForUi({ kpiId: Number(req.params.id) });

    if (!rows.length) {
      return res.status(404).json({ error: "KPI not found" });
    }

    res.json(rows[0]);
  } catch (err) {
    console.error("GET /api/kpis/:id error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// CREATE KPI
app.post("/api/kpis", async (req, res) => {
  let client;

  try {
    await ensureKpiRatioSchema();
    const {
      subject_id,
      kpi_name,
      kpi_sub_title,
      kpi_code,
      kpi_formula,
      unit,
      kpi_explanation,
      frequency,
      target_value,
      target_direction,
      tolerance_type,
      up_tolerance,
      low_tolerance,
      max_value,
      min_value,
      min_type,
      max_type,
      nombre_periode,
      calculation_on,
      calculation_mode,
      display_trend,
      regression,
      reference_kpi_id,
      target_auto_adjustment,
      owner_role_id,
      status,
      comments,
      high_limit,
      low_limit
    } = prepareKpiWritePayload(req.body);
    const resolvedSubjectId = await resolveSubjectIdForKpiPayload({
      subject_id,
      subject: req.body.subject
    });

    if (!resolvedSubjectId) {
      throw createHttpError(400, "Subject selection is required.");
    }

    client = await pool.connect();
    await client.query("BEGIN");

    const result = await client.query(
      `
      INSERT INTO public.kpi (
        kpi_name,
        kpi_sub_title,
        kpi_code,
        kpi_formula,
        unit,
        kpi_explanation,
        frequency,
        target_value,
        target_direction,
        tolerance_type,
        up_tolerance,
        low_tolerance,
        max_value,
        min_value,
        min_type,
        max_type,
        nombre_periode,
        calculation_on,
        calculation_mode,
        reference_kpi_id,
        target_auto_adjustment,
        display_trend,
        regression,
        owner_role_id,
        status,
        comments,
        high_limit,
        low_limit
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28
      )
      RETURNING *;
      `,
      [
        kpi_name,
        kpi_sub_title,
        kpi_code,
        kpi_formula,
        unit,
        kpi_explanation,
        frequency,
        target_value,
        target_direction,
        tolerance_type,
        up_tolerance,
        low_tolerance,
        max_value,
        min_value,
        min_type,
        max_type,
        nombre_periode,
        calculation_on,
        calculation_mode,
        reference_kpi_id,
        target_auto_adjustment,
        display_trend,
        regression,
        owner_role_id,
        status,
        comments,
        high_limit,
        low_limit
      ]
    );

    const newKpi = result.rows[0];
    await upsertPrimarySubjectKpiLink(client, newKpi.kpi_id, resolvedSubjectId);

    await client.query("COMMIT");
    const rows = await loadKpiRowsForUi({ kpiId: newKpi.kpi_id });
    res.status(201).json(rows[0] || newKpi);
  } catch (err) {
    if (client) {
      await client.query("ROLLBACK").catch(() => {});
    }
    console.error("POST /api/kpis error:", err.message);
    res.status(err.statusCode || 500).json({ error: err.statusCode ? err.message : "Failed to create KPI" });
  } finally {
    if (client) {
      client.release();
    }
  }
});

// UPDATE KPI
app.put("/api/kpis/:id", async (req, res) => {
  let client;

  try {
    await ensureKpiRatioSchema();
    const {
      subject_id,
      kpi_name,
      kpi_sub_title,
      kpi_code,
      kpi_formula,
      unit,
      kpi_explanation,
      frequency,
      target_value,
      target_direction,
      tolerance_type,
      up_tolerance,
      low_tolerance,
      max_value,
      min_value,
      min_type,
      max_type,
      nombre_periode,
      calculation_on,
      calculation_mode,
      display_trend,
      regression,
      reference_kpi_id,
      target_auto_adjustment,
      owner_role_id,
      status,
      comments,
      high_limit,
      low_limit
    } = prepareKpiWritePayload(req.body);
    const resolvedSubjectId = await resolveSubjectIdForKpiPayload({
      subject_id,
      subject: req.body.subject
    });

    if (!resolvedSubjectId) {
      throw createHttpError(400, "Subject selection is required.");
    }

    client = await pool.connect();
    await client.query("BEGIN");

    const result = await client.query(
      `
      UPDATE public.kpi
      SET
        kpi_name = $1,
        kpi_sub_title = $2,
        kpi_code = $3,
        kpi_formula = $4,
        unit = $5,
        kpi_explanation = $6,
        frequency = $7,
        target_value = $8,
        target_direction = $9,
        tolerance_type = $10,
        up_tolerance = $11,
        low_tolerance = $12,
        max_value = $13,
        min_value = $14,
        min_type = $15,
        max_type = $16,
        nombre_periode = $17,
        calculation_on = $18,
        calculation_mode = $19,
        display_trend = $20,
        regression = $21,
        reference_kpi_id = $22,
        target_auto_adjustment = $23,
        owner_role_id = $24,
        status = $25,
        comments = $26,
        high_limit = $27,
        low_limit = $28,
        updated_at = NOW()
      WHERE kpi_id = $29
      RETURNING *;
      `,
      [
        kpi_name,
        kpi_sub_title,
        kpi_code,
        kpi_formula,
        unit,
        kpi_explanation,
        frequency,
        target_value,
        target_direction,
        tolerance_type,
        up_tolerance,
        low_tolerance,
        max_value,
        min_value,
        min_type,
        max_type,
        nombre_periode,
        calculation_on,
        calculation_mode,
        display_trend,
        regression,
        reference_kpi_id,
        target_auto_adjustment,
        owner_role_id,
        status,
        comments,
        high_limit,
        low_limit,
        req.params.id
      ]
    );

    if (!result.rows.length) {
      await client.query("ROLLBACK").catch(() => {});
      return res.status(404).json({ error: "KPI not found" });
    }

    await upsertPrimarySubjectKpiLink(client, Number(req.params.id), resolvedSubjectId);
    await client.query("COMMIT");

    const rows = await loadKpiRowsForUi({ kpiId: Number(req.params.id) });
    res.json(rows[0] || result.rows[0]);
  } catch (err) {
    if (client) {
      await client.query("ROLLBACK").catch(() => {});
    }
    console.error("PUT /api/kpis/:id error:", err.message);
    res.status(err.statusCode || 500).json({ error: err.statusCode ? err.message : "Failed to update KPI" });
  } finally {
    if (client) {
      client.release();
    }
  }
});

// DELETE KPI
app.delete("/api/kpis/:id", async (req, res) => {
  let client;

  try {
    client = await pool.connect();
    await client.query("BEGIN");

    await client.query(
      `DELETE FROM public.kpi_target_allocation WHERE kpi_id = $1`,
      [req.params.id]
    );

    await client.query(
      `DELETE FROM public.subject_kpi WHERE kpi_id = $1`,
      [req.params.id]
    );

    const result = await client.query(
      `DELETE FROM public.kpi WHERE kpi_id = $1 RETURNING kpi_id`,
      [req.params.id]
    );

    if (!result.rows.length) {
      await client.query("ROLLBACK").catch(() => {});
      return res.status(404).json({ error: "KPI not found" });
    }

    await client.query("COMMIT");
    res.json({ success: true });
  } catch (err) {
    if (client) {
      await client.query("ROLLBACK").catch(() => {});
    }
    console.error("DELETE /api/kpis/:id error:", err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (client) {
      client.release();
    }
  }
});

// ========== KPI ADMIN PAGE ==========
// ========== KPI ADMIN PAGE ==========
app.get("/kpi-admin", async (req, res) => {
  res.send(`
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>KPI Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">

    <style>
      * {
        box-sizing: border-box;
        margin: 0;
        padding: 0;
      }

      :root {
        --bg: #f4f7fb;
        --bg-soft: #eef3f9;
        --surface: rgba(255, 255, 255, 0.88);
        --surface-strong: #ffffff;
        --border: rgba(15, 23, 42, 0.08);
        --border-strong: rgba(15, 23, 42, 0.12);
        --text: #0f172a;
        --muted: #64748b;
        --muted-2: #94a3b8;
        --primary: #2563eb;
        --primary-2: #4f46e5;
        --cyan: #06b6d4;
        --success: #10b981;
        --warning: #f59e0b;
        --danger: #ef4444;
        --shadow-sm: 0 6px 18px rgba(15, 23, 42, 0.05);
        --shadow-md: 0 14px 34px rgba(15, 23, 42, 0.08);
        --shadow-lg: 0 22px 60px rgba(15, 23, 42, 0.10);
        --radius-xl: 28px;
        --radius-lg: 22px;
        --radius-md: 16px;
        --radius-sm: 12px;
      }

      body {
        font-family: "Inter", sans-serif;
        background:
          radial-gradient(circle at top left, rgba(79,70,229,0.08), transparent 22%),
          radial-gradient(circle at top right, rgba(6,182,212,0.08), transparent 22%),
          linear-gradient(180deg, #f8fbff 0%, #f3f7fc 100%);
        color: var(--text);
        min-height: 100vh;
      }

      .page {
        max-width: 1600px;
        margin: 0 auto;
        padding: 28px;
      }

      .hero {
        position: relative;
        overflow: hidden;
        background: linear-gradient(135deg, #ffffff 0%, #f6f9ff 100%);
        border: 1px solid var(--border);
        border-radius: var(--radius-xl);
        box-shadow: var(--shadow-lg);
        padding: 32px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 20px;
        flex-wrap: wrap;
        margin-bottom: 24px;
      }

      .hero::before {
        content: "";
        position: absolute;
        width: 260px;
        height: 260px;
        border-radius: 50%;
        background: radial-gradient(circle, rgba(37,99,235,0.10), transparent 70%);
        top: -110px;
        right: -60px;
        pointer-events: none;
      }

      .hero::after {
        content: "";
        position: absolute;
        width: 200px;
        height: 200px;
        border-radius: 50%;
        background: radial-gradient(circle, rgba(6,182,212,0.10), transparent 70%);
        bottom: -90px;
        left: -40px;
        pointer-events: none;
      }

      .hero-left h1 {
        font-size: 40px;
        font-weight: 900;
        letter-spacing: -1.7px;
        color: #0b1220;
      }

      .hero-left p {
        margin-top: 12px;
        color: var(--muted);
        font-size: 15px;
        line-height: 1.7;
        max-width: 760px;
      }

      .hero-actions {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
      }

      .btn {
        border: none;
        outline: none;
        cursor: pointer;
        transition: all 0.22s ease;
        font-weight: 800;
        border-radius: 14px;
        padding: 13px 18px;
        font-size: 14px;
      }

      .btn:hover {
        transform: translateY(-2px);
      }

      .btn-primary {
        color: #fff;
        background: linear-gradient(135deg, var(--primary-2), var(--primary), var(--cyan));
        box-shadow: 0 14px 28px rgba(37, 99, 235, 0.20);
      }

      .btn-soft {
        color: var(--text);
        background: rgba(255,255,255,0.8);
        border: 1px solid var(--border-strong);
        box-shadow: var(--shadow-sm);
      }

      .btn-danger {
        color: white;
        background: linear-gradient(135deg, #f87171, #ef4444);
        box-shadow: 0 12px 22px rgba(239,68,68,0.16);
      }

      .stats-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 18px;
        margin-bottom: 24px;
      }

      .stat-card {
        position: relative;
        overflow: hidden;
        background: rgba(255,255,255,0.84);
        backdrop-filter: blur(16px);
        border: 1px solid rgba(255,255,255,0.7);
        border-radius: var(--radius-lg);
        padding: 22px;
        box-shadow: var(--shadow-md);
      }

      .stat-card::before {
        content: "";
        position: absolute;
        top: -25px;
        right: -25px;
        width: 110px;
        height: 110px;
        border-radius: 50%;
        background: linear-gradient(135deg, rgba(79,70,229,0.09), rgba(6,182,212,0.08));
      }

      .stat-label {
        color: var(--muted);
        font-size: 13px;
        font-weight: 700;
        margin-bottom: 10px;
      }

      .stat-value {
        font-size: 30px;
        font-weight: 900;
        color: #0f172a;
        letter-spacing: -1px;
      }

      .toolbar {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 16px;
        flex-wrap: wrap;
        margin-bottom: 20px;
      }

      .search-wrap {
        flex: 1;
        min-width: 280px;
        position: relative;
      }

      .search {
        width: 100%;
        border: 1px solid rgba(148,163,184,0.25);
        background: rgba(255,255,255,0.88);
        color: var(--text);
        border-radius: 18px;
        padding: 16px 18px;
        font-size: 14px;
        outline: none;
        box-shadow: var(--shadow-sm);
        transition: all 0.18s ease;
      }

      .search::placeholder {
        color: #94a3b8;
      }

      .search:focus {
        border-color: rgba(37,99,235,0.35);
        box-shadow: 0 0 0 4px rgba(37,99,235,0.10);
      }

      .table-shell {
        background: rgba(255,255,255,0.86);
        border: 1px solid rgba(255,255,255,0.72);
        border-radius: var(--radius-xl);
        overflow: hidden;
        box-shadow: var(--shadow-lg);
        backdrop-filter: blur(18px);
      }

      .table-headbar {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 20px 22px;
        border-bottom: 1px solid rgba(15,23,42,0.06);
        background: linear-gradient(180deg, #ffffff, #fafcff);
      }

      .table-headbar h3 {
        font-size: 18px;
        font-weight: 800;
        color: #0f172a;
      }

      .table-headbar span {
        font-size: 13px;
        color: var(--muted);
        font-weight: 700;
      }

      .table-wrap {
        overflow: auto;
      }

      table {
        width: 100%;
        min-width: 1300px;
        border-collapse: separate;
        border-spacing: 0;
      }

      thead th {
        position: sticky;
        top: 0;
        z-index: 2;
        text-align: left;
        padding: 18px 16px;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.14em;
        color: #64748b;
        background: #f8fbff;
        border-bottom: 1px solid rgba(15,23,42,0.06);
        white-space: nowrap;
      }

      tbody tr {
        transition: all 0.16s ease;
      }

      tbody tr:hover {
        background: #f8fbff;
      }

      tbody tr.selected {
        background: linear-gradient(90deg, rgba(37,99,235,0.06), rgba(6,182,212,0.05));
      }

      tbody td {
        padding: 18px 16px;
        border-bottom: 1px solid rgba(15,23,42,0.05);
        vertical-align: middle;
        font-size: 14px;
        color: #1e293b;
      }

      .kpi-title {
        font-weight: 800;
        font-size: 15px;
        color: #0f172a;
      }

      .kpi-subtitle {
        color: var(--muted);
        font-size: 12px;
        margin-top: 5px;
        line-height: 1.5;
      }

      .number-badge {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 54px;
        padding: 8px 12px;
        border-radius: 999px;
        background: linear-gradient(135deg, rgba(79,70,229,0.10), rgba(37,99,235,0.10));
        color: #1d4ed8;
        font-weight: 800;
      }

      .pill {
        display: inline-flex;
        align-items: center;
        padding: 8px 12px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 800;
        white-space: nowrap;
        border: 1px solid transparent;
      }

      .pill-blue {
        background: rgba(37,99,235,0.08);
        color: #1d4ed8;
        border-color: rgba(37,99,235,0.12);
      }

      .pill-indigo {
        background: rgba(79,70,229,0.08);
        color: #4338ca;
        border-color: rgba(79,70,229,0.12);
      }

      .pill-gold {
        background: rgba(245,158,11,0.10);
        color: #b45309;
        border-color: rgba(245,158,11,0.14);
      }

      .row-actions {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
      }

      .icon-btn {
        border: none;
        border-radius: 12px;
        padding: 10px 14px;
        font-size: 12px;
        font-weight: 800;
        cursor: pointer;
        transition: all 0.18s ease;
      }

      .icon-btn:hover {
        transform: translateY(-1px);
      }

      .edit-btn {
        background: rgba(37,99,235,0.08);
        color: #1d4ed8;
        border: 1px solid rgba(37,99,235,0.10);
      }

      .delete-btn {
        background: rgba(239,68,68,0.08);
        color: #dc2626;
        border: 1px solid rgba(239,68,68,0.10);
      }

      .drawer {
        position: fixed;
        top: 0;
        right: -680px;
        width: 680px;
        max-width: 100%;
        height: 100vh;
        z-index: 999;
        background: linear-gradient(180deg, #ffffff 0%, #f9fbff 100%);
        border-left: 1px solid rgba(15,23,42,0.06);
        box-shadow: -18px 0 50px rgba(15,23,42,0.10);
        transition: right 0.30s ease;
        display: flex;
        flex-direction: column;
      }

      .drawer.open {
        right: 0;
      }

      .drawer-header {
        padding: 24px 24px 18px;
        border-bottom: 1px solid rgba(15,23,42,0.06);
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        background: rgba(255,255,255,0.92);
        backdrop-filter: blur(12px);
      }

      .drawer-header h2 {
        font-size: 26px;
        font-weight: 900;
        color: #0f172a;
      }

      .drawer-body {
        padding: 22px 24px 130px;
        overflow: auto;
      }

      .section-title {
        margin: 24px 0 12px;
        font-size: 12px;
        font-weight: 900;
        color: #4f46e5;
        text-transform: uppercase;
        letter-spacing: 0.14em;
      }

      .form-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 14px;
      }

      .field {
        display: flex;
        flex-direction: column;
        gap: 8px;
      }

   .hierarchy-tree-field {
      grid-column: 1 / -1;
      width: 100%;
      }

     .tree-select-panel {
      width: 100%;
     }

     .tree-list {
      width: 100%;
     }

      .field.full {
        grid-column: 1 / -1;
      }

      .field label {
        font-size: 12px;
        font-weight: 800;
        color: #334155;
      }

      .field input,
      .field textarea,
      .field select {
        width: 100%;
        border: 1px solid rgba(148,163,184,0.24);
        background: #ffffff;
        color: #0f172a;
        border-radius: 16px;
        padding: 13px 14px;
        font-size: 14px;
        font-family: inherit;
        outline: none;
        transition: all 0.18s ease;
        box-shadow: 0 2px 6px rgba(15,23,42,0.03);
      }

      .field input:focus,
      .field textarea:focus,
      .field select:focus {
        border-color: rgba(37,99,235,0.34);
        box-shadow: 0 0 0 4px rgba(37,99,235,0.10);
      }

      .field textarea {
        min-height: 110px;
        resize: vertical;
      }

      .drawer-footer {
        position: absolute;
        left: 0;
        right: 0;
        bottom: 0;
        padding: 18px 24px;
        background: rgba(255,255,255,0.96);
        border-top: 1px solid rgba(15,23,42,0.06);
        display: flex;
        justify-content: space-between;
        gap: 12px;
      }

      .toast {
        position: fixed;
        right: 24px;
        bottom: 24px;
        background: #ffffff;
        color: #0f172a;
        padding: 14px 18px;
        border-radius: 14px;
        display: none;
        z-index: 9999;
        border: 1px solid rgba(15,23,42,0.08);
        box-shadow: var(--shadow-md);
      }

      .empty-state {
        text-align: center;
        padding: 56px 20px;
        color: var(--muted);
        font-size: 14px;
      }

      ::-webkit-scrollbar {
        width: 10px;
        height: 10px;
      }

      ::-webkit-scrollbar-thumb {
        background: rgba(148,163,184,0.35);
        border-radius: 999px;
      }

      ::-webkit-scrollbar-track {
        background: transparent;
      }

      @media (max-width: 1100px) {
        .stats-grid {
          grid-template-columns: repeat(2, minmax(0, 1fr));
        }
      }

      @media (max-width: 900px) {
        .page {
          padding: 16px;
        }

        .hero {
          padding: 24px;
        }

        .hero-left h1 {
          font-size: 30px;
        }

        .drawer {
          width: 100%;
          right: -100%;
        }

        .form-grid {
          grid-template-columns: 1fr;
        }

        .stats-grid {
          grid-template-columns: 1fr;
        }
      }
    </style>
  </head>
  <body>
    <div class="page">
      <div class="hero">
        <div class="hero-left">
          <h1>KPI Management Dashboard</h1>
          <p>
            A clean and professional KPI workspace to create, update, manage and delete all your performance indicators with a premium enterprise design.
          </p>
        </div>
        <div class="hero-actions">
          <button class="btn btn-soft" onclick="refreshTable()">Refresh</button>
          <button class="btn btn-primary" onclick="openNewDrawer()">+ Add KPI</button>
        </div>
      </div>

      <div class="stats-grid">
        <div class="stat-card">
          <div class="stat-label">Total KPIs</div>
          <div class="stat-value" id="statTotal">0</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Monthly KPIs</div>
          <div class="stat-value" id="statMonthly">0</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">With Targets</div>
          <div class="stat-value" id="statTargets">0</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Tolerance Rules</div>
          <div class="stat-value" id="statTolerance">0</div>
        </div>
      </div>

      <div class="toolbar">
        <div class="search-wrap">
          <input id="search" class="search" placeholder="Search by title, subtitle, subject, frequency..." />
        </div>
      </div>

      <div class="table-shell">
        <div class="table-headbar">
          <h3>KPI Master List</h3>
          <span>Total rows: <strong id="rowCount">0</strong></span>
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Indicator</th>
                <th>Subject</th>
                <th>Unit</th>
                <th>Frequency</th>
                <th>Target</th>
                <th>Low Limit</th>
                <th>High Limit</th>
                <th>Tolerance Type</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody id="tableBody">
              <tr><td colspan="10" class="empty-state">Loading KPIs...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div id="drawer" class="drawer">
      <div class="drawer-header">
        <h2 id="drawerTitle">Edit KPI</h2>
        <button class="btn btn-soft" onclick="closeDrawer()">Close</button>
      </div>

      <div class="drawer-body">
        <input type="hidden" id="kpi_id" />

        <div class="section-title">Identity</div>
        <div class="form-grid">
          <div class="field">
            <label>Category</label>
            <input id="indicator_title" />
          </div>
          <div class="field">
            <label>KPI</label>
            <input id="indicator_sub_title" />
          </div>
          <div class="field">
            <label>Subject</label>
            <input id="subject" />
          </div>
          <div class="field">
            <label>Unit</label>
            <input id="unit" />
          </div>
          <div class="field full">
            <label>Definition</label>
            <textarea id="definition"></textarea>
          </div>
        </div>

        <div class="section-title">Performance Rules</div>
        <div class="form-grid">
          <div class="field">
            <label>Frequency</label>
            <input id="frequency" />
          </div>
          <div class="field">
            <label>Calculation On</label>
            <select id="calculation_on">
              <option value="Value">Value</option>
              <option value="Average">Average</option>
            </select>
          </div>
          <div class="field">
            <label>Target</label>
            <input id="target" />
          </div>
          <div class="field">
            <label>Target Auto Adjustment</label>
            <select id="target_auto_adjustment">
              <option value="Yes">Yes</option>
              <option value="No">No</option>
            </select>
          </div>
        </div>

        <div class="section-title">Limits & Tolerances</div>
        <div class="form-grid">
          <div class="field">
            <label>Tolerance Type</label>
            <input id="tolerance_type" />
          </div>
          <div class="field">
            <label>Up Tolerance</label>
            <input id="up_tolerance" />
          </div>
          <div class="field">
            <label>Low Tolerance</label>
            <input id="low_tolerance" />
          </div>
          <div class="field">
            <label>High Limit</label>
            <input id="high_limit" type="number" step="any" />
          </div>
          <div class="field">
            <label>Low Limit</label>
            <input id="low_limit" type="number" step="any" />
          </div>
          <div class="field">
            <label>Max</label>
            <input id="max" />
          </div>
          <div class="field">
            <label>Min</label>
            <input id="min" />
          </div>
        </div>
      </div>

  <div class="modal-footer">
  <div class="footer-actions">
    <button class="btn btn-soft" onclick="closeModal()">Cancel</button>
    <button class="btn btn-primary" onclick="saveKpi()">Save KPI</button>
  </div>
</div>
    </div>

    <div id="toast" class="toast"></div>

    <script>
      let currentRows = [];
      let selectedKpiId = null;

      function showToast(message) {
        const toast = document.getElementById("toast");
        toast.textContent = message;
        toast.style.display = "block";
        setTimeout(() => {
          toast.style.display = "none";
        }, 2400);
      }

      function escapeHtml(value) {
        return String(value ?? "")
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/"/g, "&quot;")
          .replace(/'/g, "&#39;");
      }

      function updateStats(rows) {
        const total = rows.length;
        const monthly = rows.filter(r => String(r.frequency || "").toLowerCase().includes("month")).length;
        const withTargets = rows.filter(r => r.target !== null && r.target !== undefined && String(r.target).trim() !== "").length;
        const tolerance = rows.filter(r => r.tolerance_type && String(r.tolerance_type).trim() !== "").length;

        document.getElementById("statTotal").textContent = total;
        document.getElementById("statMonthly").textContent = monthly;
        document.getElementById("statTargets").textContent = withTargets;
        document.getElementById("statTolerance").textContent = tolerance;
        document.getElementById("rowCount").textContent = total;
      }

      function renderTable(rows) {
        currentRows = rows || [];
        updateStats(currentRows);

        const tbody = document.getElementById("tableBody");

        if (!currentRows.length) {
          tbody.innerHTML = '<tr><td colspan="10" class="empty-state">No KPI found.</td></tr>';
          return;
        }

        tbody.innerHTML = currentRows.map(row => \`
          <tr class="\${String(selectedKpiId) === String(row.kpi_id) ? 'selected' : ''}">
            <td><span class="number-badge">#\${escapeHtml(row.kpi_id)}</span></td>
            <td>
              <div class="kpi-title">\${escapeHtml(row.indicator_title || 'Untitled KPI')}</div>
              <div class="kpi-subtitle">\${escapeHtml(row.indicator_sub_title || '')}</div>
            </td>
            <td>\${escapeHtml(row.subject || '')}</td>
            <td><span class="pill pill-blue">\${escapeHtml(row.unit || '-')}</span></td>
            <td><span class="pill pill-indigo">\${escapeHtml(row.frequency || '-')}</span></td>
            <td>\${escapeHtml(row.target || '-')}</td>
            <td>\${escapeHtml(row.low_limit ?? '-')}</td>
            <td>\${escapeHtml(row.high_limit ?? '-')}</td>
            <td><span class="pill pill-gold">\${escapeHtml(row.tolerance_type || '-')}</span></td>
            <td>
              <div class="row-actions">
                <button class="icon-btn edit-btn" onclick="openEditDrawer(\${row.kpi_id})">Edit</button>
                <button class="icon-btn delete-btn" onclick="deleteRowQuick(\${row.kpi_id})">Delete</button>
              </div>
            </td>
          </tr>
        \`).join("");
      }

      async function loadKpis(search = "") {
        try {
          const res = await fetch('/api/kpis?search=' + encodeURIComponent(search));
          const data = await res.json();
          renderTable(data);
        } catch (err) {
          document.getElementById("tableBody").innerHTML =
            '<tr><td colspan="10" class="empty-state">Failed to load KPI table.</td></tr>';
        }
      }

      async function loadKpiCharts() {
  const chartsGrid = document.getElementById("chartsGrid");

  try {
    const res = await fetch('/api/responsibles/' + responsibleId + '/kpi-graphs');
    if (!res.ok) throw new Error("Failed to load chart data");

    const rows = await res.json();

    if (!rows.length) {
      chartsGrid.innerHTML = '<div class="empty">No KPI graph data found for this responsible.</div>';
      return;
    }

    chartsGrid.innerHTML = rows.map((row, index) => \`
      <div class="chart-card">
        <h3>\${escapeHtml(row.indicator_title || "Untitled KPI")}</h3>
        <p>\${escapeHtml(row.indicator_sub_title || "Weekly KPI trend")}</p>
        <div class="chart-wrap">
          <canvas id="chart_\${index}"></canvas>
        </div>
      </div>
    \`).join("");

    chartInstances.forEach(chart => chart.destroy());
    chartInstances = [];

    rows.forEach((row, index) => {
      const canvas = document.getElementById('chart_' + index);
      const ctx = canvas.getContext('2d');
      const axisValues = [
        ...(Array.isArray(row.values) ? row.values : []),
        ...(Array.isArray(row.target) ? row.target : []),
        ...(Array.isArray(row.highLimits) ? row.highLimits : []),
        ...(Array.isArray(row.lowLimits) ? row.lowLimits : [])
      ]
        .filter((value) => value !== null && value !== undefined && value !== '')
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value));
      const axisSourceMin = axisValues.length ? Math.min(...axisValues) : 0;
      const axisSourceMax = axisValues.length ? Math.max(...axisValues) : 100;
      let axisMin = axisSourceMin > 0
        ? axisSourceMin * 0.8
        : axisSourceMin < 0
          ? axisSourceMin * 1.2
          : 0;
      let axisMax = axisSourceMax > 0
        ? axisSourceMax * 1.2
        : axisSourceMax < 0
          ? axisSourceMax * 0.8
          : 0;
      if (axisSourceMin === axisSourceMax) {
        const pad = Math.max(Math.abs(axisSourceMax || axisSourceMin || 1) * 0.2, 1);
        axisMin = axisSourceMin - pad;
        axisMax = axisSourceMax + pad;
      }

    const chart = new Chart(ctx, {
  data: {
    labels: row.labels,
    datasets: [
      {
        type: 'bar',
        label: 'Actual Value',
        data: row.values,
        borderWidth: 0,
        backgroundColor: 'rgba(34, 197, 94, 0.85)',
        borderRadius: 6,
        barThickness: 26
      },
      {
        type: 'line',
        label: 'Target',
        data: row.target,
        borderColor: 'rgba(239, 68, 68, 0.95)',
        backgroundColor: 'rgba(239, 68, 68, 0.15)',
        borderWidth: 2,
        tension: 0,
        fill: false,
        pointRadius: 0,
        pointHoverRadius: 4,
        borderDash: [6, 4]
      },
      {
        type: 'line',
        label: 'High Limit',
        data: row.highLimits,
        borderColor: 'rgba(245, 158, 11, 0.95)',
        backgroundColor: 'rgba(245, 158, 11, 0.15)',
        borderWidth: 2,
        tension: 0,
        fill: false,
        pointRadius: 0,
        pointHoverRadius: 4,
        borderDash: [8, 5]
      },
      {
        type: 'line',
        label: 'Low Limit',
        data: row.lowLimits,
        borderColor: 'rgba(59, 130, 246, 0.95)',
        backgroundColor: 'rgba(59, 130, 246, 0.15)',
        borderWidth: 2,
        tension: 0,
        fill: false,
        pointRadius: 0,
        pointHoverRadius: 4,
        borderDash: [8, 5]
      }
    ]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: true
      },
      tooltip: {
        mode: 'index',
        intersect: false
      }
    },
    interaction: {
      mode: 'nearest',
      axis: 'x',
      intersect: false
    },
    scales: {
      x: {
        grid: {
          display: false
        },
        ticks: {
          color: '#64748b'
        }
      },
      y: {
        beginAtZero: false,
        min: axisMin,
        max: axisMax,
        ticks: {
          color: '#64748b'
        },
        grid: {
          color: 'rgba(148,163,184,0.15)'
        }
      }
    }
  }
});
      chartInstances.push(chart);
      });
    } catch (error) {
    chartsGrid.innerHTML = '<div class="empty">Failed to load KPI charts.</div>';
     }
   }


      async function refreshTable() {
        await loadKpis(document.getElementById("search").value || "");
        showToast("Dashboard refreshed");
      }

      function openDrawer() {
        document.getElementById("drawer").classList.add("open");
      }

      function closeDrawer() {
        document.getElementById("drawer").classList.remove("open");
      }

      function formatToleranceForInput(value, toleranceType) {
        if (value === null || value === undefined || value === "") return "";
        const text = String(value).trim();
        if (!text) return "";
        return String(toleranceType || "").trim().toLowerCase() === "relative" && !text.includes("%")
          ? text + "%"
          : text;
      }

    function handleToleranceTypeChange(source) {
     syncToleranceInputs(source);

        const toleranceType = document.getElementById("tolerance_type").value;

        if (String(toleranceType).trim().toLowerCase() === "relative") {
        recalculateLimits(source);
        }
    }

    function resetForm() {
      const fields = [
        "kpi_id","indicator_title","indicator_sub_title","unit","subject","definition",
        "frequency","target","tolerance_type","up_tolerance","low_tolerance",
        "max","min","calculation_on","target_auto_adjustment","high_limit","low_limit"
      ];

      fields.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = "";
      });

      document.getElementById("calculation_on").value = "Value";
      document.getElementById("target_auto_adjustment").value = "Yes";
      document.getElementById("tolerance_type").value = "Relative";
      handleToleranceTypeChange();
    }

    function fillForm(data) {
     const fields = [
    "kpi_id","indicator_title","indicator_sub_title","unit","subject","definition",
    "frequency","target","tolerance_type","up_tolerance","low_tolerance",
    "max","min","calculation_on","target_auto_adjustment","high_limit","low_limit"
    ];

    fields.forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = data[id] ?? "";
    });

    handleToleranceTypeChange();
  }
        document.getElementById("up_tolerance").value = formatToleranceForInput(data.up_tolerance, data.tolerance_type);
        document.getElementById("low_tolerance").value = formatToleranceForInput(data.low_tolerance, data.tolerance_type);
        handleToleranceTypeChange();
      }

      function openNewDrawer() {
        selectedKpiId = null;
        resetForm();
        document.getElementById("drawerTitle").textContent = "Create KPI";
        openDrawer();
      }

      async function openEditDrawer(id) {
        try {
          const res = await fetch('/api/kpis/' + id);
          const data = await res.json();
          selectedKpiId = data.kpi_id;
          fillForm(data);
          document.getElementById("drawerTitle").textContent = "Edit KPI #" + data.kpi_id;
          openDrawer();
          await loadKpis(document.getElementById("search").value || "");
        } catch (err) {
          showToast("Unable to load KPI");
        }
      }


    

      function buildPayload() {
        return {
          indicator_title: document.getElementById("indicator_title").value,
          indicator_sub_title: document.getElementById("indicator_sub_title").value,
          unit: document.getElementById("unit").value,
          subject: document.getElementById("subject").value,
          definition: document.getElementById("definition").value,
          frequency: document.getElementById("frequency").value,
          target: getSafeValue("target"),
          tolerance_type: document.getElementById("tolerance_type").value,
          up_tolerance: document.getElementById("up_tolerance").value,
          low_tolerance: document.getElementById("low_tolerance").value,
          max: document.getElementById("max").value,
          min: document.getElementById("min").value,
          target_direction: document.getElementById("direction").value,
          nombre_periode: document.getElementById("nombre_periode").value,
          calculation_mode: document.getElementById("calculation_mode").value,
          display_trend: document.getElementById("display_trend").value,
          regression: document.getElementById("regression").value,
          min_type: document.getElementById("min_type").value,
          max_type: document.getElementById("max_type").value,
          calculation_on: document.getElementById("calculation_on").value,
          target_auto_adjustment: document.getElementById("target_auto_adjustment").value,
          high_limit: document.getElementById("high_limit").value || null,
          low_limit: document.getElementById("low_limit").value || null
        };
      }

      async function saveKpi() {
        const id = document.getElementById("kpi_id").value;
        const method = id ? "PUT" : "POST";
        const url = id ? '/api/kpis/' + id : '/api/kpis';

        try {
          const res = await fetch(url, {
            method,
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(buildPayload())
          });

          if (!res.ok) {
            const errorData = await res.json().catch(() => null);
            showToast(errorData?.error || "Save failed");
            return;
          }

          const saved = await res.json();
          selectedKpiId = saved.kpi_id;
          document.getElementById("kpi_id").value = saved.kpi_id;
          await loadKpis(document.getElementById("search").value || "");
          showToast("KPI saved successfully");
          closeDrawer();
        } catch (err) {
          console.error("SAVE KPI ERROR:", error);
          showToast("Save failed: " + error.message);
        }
      }

      async function deleteKpi() {
        const id = document.getElementById("kpi_id").value;
        if (!id) {
          showToast("Select a KPI first");
          return;
        }

        const ok = confirm("Delete this KPI?");
        if (!ok) return;

        try {
          const res = await fetch('/api/kpis/' + id, { method: "DELETE" });
          if (!res.ok) {
            showToast("Delete failed");
            return;
          }

          resetForm();
          selectedKpiId = null;
          closeDrawer();
          await loadKpis(document.getElementById("search").value || "");
          showToast("KPI deleted");
        } catch (err) {
          showToast("Delete failed");
        }
      }

      async function deleteRowQuick(id) {
        const ok = confirm("Delete KPI #" + id + " ?");
        if (!ok) return;

        try {
          const res = await fetch('/api/kpis/' + id, { method: "DELETE" });
          if (!res.ok) {
            showToast("Delete failed");
            return;
          }

          if (String(document.getElementById("kpi_id").value) === String(id)) {
            resetForm();
            closeDrawer();
          }

          await loadKpis(document.getElementById("search").value || "");
          showToast("KPI deleted");
        } catch (err) {
          showToast("Delete failed");
        }
      }

      document.getElementById("search").addEventListener("input", (e) => {
        loadKpis(e.target.value);
      });

      const parameterSearchInput = document.getElementById("parameterSearch");
      if (parameterSearchInput) {
        parameterSearchInput.addEventListener("input", (e) => {
          applyParameterSearch(e.target.value);
        });
      }

      loadKpis();
    </script>
  </body>
  </html>
  `);
});


app.get("/api/responsibles/:responsibleId/kpi-graphs", async (req, res) => {
  const { responsibleId } = req.params;

  try {
    const result = await pool.query(
      `
      SELECT
          k.kpi_id,
          k.indicator_title,
          k.indicator_sub_title,
          kv.week,
          kv.value
      FROM public.kpi_values kv
      JOIN public."Kpi" k
        ON k.kpi_id = kv.kpi_id
      WHERE kv.responsible_id = $1
      ORDER BY k.kpi_id ASC, kv.week ASC
      `,
      [responsibleId]
    );

    const result1 = await pool.query(
      `SELECT name FROM public."Responsible" WHERE responsible_id = $1`,
      [responsibleId]
    );

    const responsibleName = result1.rows[0]?.name || "Unknown";


    const grouped = {};

    for (const row of result.rows) {
      if (!grouped[row.kpi_id]) {
        grouped[row.kpi_id] = {
          kpi_id: row.kpi_id,
          indicator_title: row.indicator_title,
          indicator_sub_title: row.indicator_sub_title,
          labels: [],
          values: [],
          targetValue: row.target !== null ? Number(row.target) : null,
          highLimitValue: row.high_limit !== null ? Number(row.high_limit) : null,
          lowLimitValue: row.low_limit !== null ? Number(row.low_limit) : null
        };
      }

      grouped[row.kpi_id].labels.push(row.month_label);
      grouped[row.kpi_id].values.push(Number(row.new_value_num) || 0);
    }

    res.json(Object.values(grouped));
  } catch (error) {
    console.error("GET KPI graphs error:", error);
    res.status(500).json({ error: "Failed to load KPI graph data" });
  }
});

app.post("/api/responsibles/:responsibleId/kpis", async (req, res) => {
  const { responsibleId } = req.params;
  let client;

  try {
    await ensureKpiRatioSchema();
    const responsibleContext = await loadFormResponsibleContext(responsibleId);
    const responsiblePeopleId = normalizeOptionalIntegerInput(
      responsibleContext?.people_id ?? responsibleId
    );
    const {
      subject_id,
      kpi_name,
      kpi_sub_title,
      kpi_code,
      kpi_formula,
      unit,
      kpi_explanation,
      frequency,
      target_value,
      target_direction,
      tolerance_type,
      up_tolerance,
      low_tolerance,
      max_value,
      min_value,
      nombre_periode,
      calculation_mode,
      display_trend,
      regression,
      min_type,
      max_type,
      calculation_on,
      target_auto_adjustment,
      reference_kpi_id,
      owner_role_id,
      status,
      comments,
      high_limit,
      low_limit
    } = prepareKpiWritePayload(req.body);
    const resolvedSubjectId = await resolveSubjectIdForKpiPayload({
      subject_id,
      subject: req.body.subject
    });

    if (!resolvedSubjectId) {
      throw createHttpError(400, "Subject selection is required.");
    }

    client = await pool.connect();
    await client.query("BEGIN");

    const insertKpi = await client.query(
      `
  INSERT INTO public.kpi (
  kpi_name,
  kpi_sub_title,
  kpi_code,
  kpi_formula,
  unit,
  kpi_explanation,
  frequency,
  target_value,
  target_direction,
  tolerance_type,
  up_tolerance,
  low_tolerance,
  max_value,
  min_value,
  nombre_periode,
  calculation_mode,
  display_trend,
  regression,
  min_type,
  max_type,
  calculation_on,
  target_auto_adjustment,
  reference_kpi_id,
  owner_role_id,
  status,
  comments,
  high_limit,
  low_limit
)
VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8,
  $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28
)
      RETURNING *
      `,
      [
        kpi_name,
        kpi_sub_title,
        kpi_code,
        kpi_formula,
        unit,
        kpi_explanation,
        frequency,
        target_value,
        target_direction,
        tolerance_type,
        up_tolerance,
        low_tolerance,
        max_value,
        min_value,
        nombre_periode,
        calculation_mode,
        display_trend,
        regression,
        min_type,
        max_type,
        calculation_on,
        target_auto_adjustment,
        reference_kpi_id,
        owner_role_id,
        status,
        comments,
        high_limit,
        low_limit
      ]
    );

    const newKpi = insertKpi.rows[0];
    await upsertPrimarySubjectKpiLink(client, newKpi.kpi_id, resolvedSubjectId);

    await client.query(
      `
      INSERT INTO public.kpi_target_allocation (
        kpi_id,
        target_value,
        target_unit,
        local_currency,
        kpi_type,
        target_setup_date,
        target_status,
        set_by_people_id,
        comments
      )
      VALUES ($1, $2, $3, $4, $5, CURRENT_DATE, $6, $7, $8)
      `,
      [
        newKpi.kpi_id,
        newKpi.target_value ?? 0,
        newKpi.unit || null,
        null,
        null,
        "Draft",
        responsiblePeopleId,
        "Auto-created from KPI creation."
      ]
    );

    await client.query("COMMIT");
    const rows = await loadKpiRowsForUi({
      kpiId: newKpi.kpi_id,
      responsibleId: responsiblePeopleId
    });
    res.status(201).json(rows[0] || newKpi);
  } catch (error) {
    if (client) {
      await client.query("ROLLBACK").catch(() => { });
    }
    console.error(error);
    res.status(error.statusCode || 500).json({ error: error.statusCode ? error.message : "Failed to create KPI" });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.get("/api/responsibles/:responsibleId/kpis", async (req, res) => {
  const { responsibleId } = req.params;
  try {
    await ensureKpiRatioSchema();
    const rows = await loadKpiRowsForUi({
      search: req.query.search || "",
      responsibleId
    });
    res.json(rows);
  } catch (error) {
    console.error("GET /api/responsibles/:responsibleId/kpis error:", error);
    res.status(500).json({ error: "Failed to load KPIs" });
  }
});

app.get("/api/responsibles/:responsibleId/kpis/:kpiId", async (req, res) => {
  const { responsibleId, kpiId } = req.params;

  try {
    await ensureKpiRatioSchema();
    const rows = await loadKpiRowsForUi({
      kpiId: Number(kpiId),
      responsibleId
    });

    if (!rows.length) {
      return res.status(404).json({ error: "KPI not found" });
    }

    res.json(rows[0]);
  } catch (error) {
    console.error("GET one KPI error:", error);
    res.status(500).json({ error: "Failed to load KPI" });
  }
});

app.put("/api/responsibles/:responsibleId/kpis/:kpiId", async (req, res) => {
  const { kpiId } = req.params;
  let client;

  try {
    await ensureKpiRatioSchema();
    const {
      subject_id,
      kpi_name,
      kpi_sub_title,
      kpi_code,
      kpi_formula,
      unit,
      kpi_explanation,
      frequency,
      target_value,
      target_direction,
      tolerance_type,
      up_tolerance,
      low_tolerance,
      max_value,
      min_value,
      nombre_periode,
      calculation_mode,
      display_trend,
      regression,
      min_type,
      max_type,
      calculation_on,
      target_auto_adjustment,
      reference_kpi_id,
      owner_role_id,
      status,
      comments,
      high_limit,
      low_limit
    } = prepareKpiWritePayload(req.body);
    const resolvedSubjectId = await resolveSubjectIdForKpiPayload({
      subject_id,
      subject: req.body.subject
    });

    if (!resolvedSubjectId) {
      throw createHttpError(400, "Subject selection is required.");
    }

    client = await pool.connect();
    await client.query("BEGIN");

    const result = await client.query(
      `
      UPDATE public.kpi
      SET
          kpi_name = $1,
          kpi_sub_title = $2,
          kpi_code = $3,
          kpi_formula = $4,
          unit = $5,
          kpi_explanation = $6,
          frequency = $7,
          target_value = $8,
          target_direction = $9,
          tolerance_type = $10,
          up_tolerance = $11,
          low_tolerance = $12,
          max_value = $13,
          min_value = $14,
          nombre_periode = $15,
          calculation_mode = $16,
          display_trend = $17,
          regression = $18,
          min_type = $19,
          max_type = $20,
          calculation_on = $21,
          target_auto_adjustment = $22,
          reference_kpi_id = $23,
          owner_role_id = $24,
          status = $25,
          comments = $26,
          high_limit = $27,
          low_limit = $28,
          updated_at = NOW()
      WHERE kpi_id = $29
      RETURNING *
      `,
      [
        kpi_name,
        kpi_sub_title,
        kpi_code,
        kpi_formula,
        unit,
        kpi_explanation,
        frequency,
        target_value,
        target_direction,
        tolerance_type,
        up_tolerance,
        low_tolerance,
        max_value,
        min_value,
        nombre_periode,
        calculation_mode,
        display_trend,
        regression,
        min_type,
        max_type,
        calculation_on,
        target_auto_adjustment,
        reference_kpi_id,
        owner_role_id,
        status,
        comments,
        high_limit,
        low_limit,
        kpiId
      ]
    );

    if (!result.rows.length) {
      await client.query("ROLLBACK").catch(() => {});
      return res.status(404).json({ error: "KPI not found" });
    }

    await upsertPrimarySubjectKpiLink(client, Number(kpiId), resolvedSubjectId);
    await client.query("COMMIT");

    const rows = await loadKpiRowsForUi({ kpiId: Number(kpiId) });
    res.json(rows[0] || result.rows[0]);
  } catch (error) {
    if (client) {
      await client.query("ROLLBACK").catch(() => {});
    }
    console.error(error);
    res.status(error.statusCode || 500).json({ error: error.statusCode ? error.message : "Failed to update KPI" });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.delete("/api/responsibles/:responsibleId/kpis/:kpiId", async (req, res) => {
  const { kpiId } = req.params;
  let client;

  try {
    client = await pool.connect();
    await client.query("BEGIN");

    await client.query(
      `DELETE FROM public.kpi_target_allocation WHERE kpi_id = $1`,
      [kpiId]
    );

    await client.query(
      `DELETE FROM public.subject_kpi WHERE kpi_id = $1`,
      [kpiId]
    );

    const result = await client.query(
      `
      DELETE FROM public.kpi
      WHERE kpi_id = $1
      RETURNING *
      `,
      [kpiId]
    );

    if (!result.rows.length) {
      await client.query("ROLLBACK").catch(() => {});
      return res.status(404).json({ error: "KPI not found" });
    }

    await client.query("COMMIT");
    res.json({ success: true });
  } catch (error) {
    if (client) {
      await client.query("ROLLBACK").catch(() => {});
    }
    console.error(error);
    res.status(500).json({ error: "Failed to delete KPI" });
  } finally {
    if (client) {
      client.release();
    }
  }
});

app.get("/api/responsibles/:responsibleId/parameter-kpis", async (req, res) => {
  const { responsibleId } = req.params;

  try {
    await ensureResponsibleParameterKpiSchema();

    const result = await pool.query(
      `
      SELECT
        pk.parameter_kpi_id,
        pk.kpi_id,
        pk.location,
        pk.local_currency,
        pk.value,
        pk.value_date,
        pk.target,
        pk.best_new_target,
        pk.kpi_type,
        pk.created_at,
        pk.updated_at,
        r.name AS responsible_name,
        k.subject AS kpi_subject,
        k.indicator_title AS kpi_group,
        k.indicator_sub_title AS kpi_name
      FROM public.responsible_parameter_kpis pk
      JOIN public."Responsible" r
        ON r.responsible_id = pk.responsible_id
      LEFT JOIN public."Kpi" k
        ON k.kpi_id = pk.kpi_id
      WHERE pk.responsible_id = $1
      ORDER BY pk.created_at DESC, pk.parameter_kpi_id DESC
      `,
      [responsibleId]
    );

    res.json(result.rows);
  } catch (error) {
    console.error("GET parameter KPIs error:", error);
    res.status(500).json({ error: "Failed to load parameter KPIs" });
  }
});

app.get("/api/responsibles/:responsibleId/parameter-kpis/:parameterKpiId", async (req, res) => {
  const { responsibleId, parameterKpiId } = req.params;

  try {
    await ensureResponsibleParameterKpiSchema();

    const result = await pool.query(
      `
      SELECT
        pk.parameter_kpi_id,
        pk.kpi_id,
        pk.location,
        pk.local_currency,
        pk.value,
        pk.value_date,
        pk.target,
        pk.best_new_target,
        pk.kpi_type,
        pk.created_at,
        pk.updated_at,
        r.name AS responsible_name,
        k.subject AS kpi_subject,
        k.indicator_title AS kpi_group,
        k.indicator_sub_title AS kpi_name
      FROM public.responsible_parameter_kpis pk
      JOIN public."Responsible" r
        ON r.responsible_id = pk.responsible_id
      LEFT JOIN public."Kpi" k
        ON k.kpi_id = pk.kpi_id
      WHERE pk.responsible_id = $1
        AND pk.parameter_kpi_id = $2
      LIMIT 1
      `,
      [responsibleId, parameterKpiId]
    );

    if (!result.rows.length) {
      return res.status(404).json({ error: "Parameter KPI not found" });
    }

    res.json(result.rows[0]);
  } catch (error) {
    console.error("GET one parameter KPI error:", error);
    res.status(500).json({ error: "Failed to load parameter KPI" });
  }
});

app.post("/api/responsibles/:responsibleId/parameter-kpis", async (req, res) => {
  const { responsibleId } = req.params;

  try {
    await ensureResponsibleParameterKpiSchema();

    const {
      kpi_id,
      location,
      local_currency,
      value,
      value_date,
      target,
      best_new_target,
      kpi_type
    } = prepareResponsibleParameterKpiPayload(req.body);

    const result = await pool.query(
      `
      INSERT INTO public.responsible_parameter_kpis (
        responsible_id,
        kpi_id,
        location,
        local_currency,
        value,
        value_date,
        target,
        best_new_target,
        kpi_type
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      RETURNING *
      `,
      [
        responsibleId,
        kpi_id,
        location,
        local_currency,
        value,
        value_date,
        target,
        best_new_target,
        kpi_type
      ]
    );

    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("POST parameter KPI error:", error);
    res.status(error.statusCode || 500).json({
      error: error.statusCode ? error.message : "Failed to create parameter KPI"
    });
  }
});

app.post("/api/responsibles/:responsibleId/kpi-objects", async (req, res) => {
  const { responsibleId } = req.params;
  try {
    await ensureKpiObjectSchema();
    const responsiblePeopleContext = await loadPeopleContextByPeopleId(responsibleId);
    const normalizedResponsiblePeopleId =
      responsiblePeopleContext?.people_id ||
      normalizeOptionalIntegerInput(responsibleId);
    const {
      kpi_id,
      plant_id,
      zone_id,
      unit_id,
      function: functionName,
      product_id,
      product_line_id,
      market_id,
      customer_id,
      target_value,
      target_unit,
      local_currency,
      kpi_type,
      target_setup_date,
      target_start_date,
      target_end_date,
      last_best_target,
      previous_target_value,
      target_status,
      set_by_people_id,
      approved_by_people_id,
      comments
    } = prepareKpiObjectPayload({
      ...req.body,
      set_by_people_id: req.body.set_by_people_id || normalizedResponsiblePeopleId
    });

    const result = await pool.query(
      `
      INSERT INTO public.kpi_target_allocation (
      kpi_id,
      plant_id,
      zone_id,
      unit_id,
      "function",
      product_id,
      product_line_id,
      market_id,
      customer_id,
      target_value,
      target_unit,
      local_currency,
      kpi_type,
      target_setup_date,
      target_start_date,
      target_end_date,
      last_best_target,
      previous_target_value,
      target_status,
      set_by_people_id,
      approved_by_people_id,
      comments
     )
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
      RETURNING *
      `,
      [
        kpi_id,
        plant_id,
        zone_id,
        unit_id,
        functionName,
        product_id,
        product_line_id,
        market_id,
        customer_id,
        target_value,
        target_unit,
        local_currency,
        kpi_type,
        target_setup_date,
        target_start_date,
        target_end_date,
        last_best_target,
        previous_target_value,
        target_status,
        set_by_people_id,
        approved_by_people_id,
        comments
      ]
    );

    const rows = await loadKpiTargetAllocationRowsForUi({
      allocationId: result.rows[0]?.kpi_target_allocation_id,
      responsibleId: normalizedResponsiblePeopleId || responsibleId
    });

    res.status(201).json(rows[0] || result.rows[0]);
  } catch (error) {
    console.error("POST KPI object error:", error);
    res.status(error.statusCode || 500).json({
      error: error.statusCode ? error.message : "Failed to create KPI target allocation"
    });
  }
});

app.get("/api/responsibles/:responsibleId/kpi-objects", async (req, res) => {
  const { responsibleId } = req.params;
  try {
    await ensureKpiObjectSchema();
    const rows = await loadKpiTargetAllocationRowsForUi({ responsibleId });
    res.json(rows);
  } catch (error) {
    console.error("GET KPI objects error:", error);
    res.status(500).json({ error: "Failed to load KPI target allocations" });
  }
});

app.get("/api/responsibles/:responsibleId/kpi-objects/:kpiObjectId", async (req, res) => {
  const { responsibleId, kpiObjectId } = req.params;

  try {
    await ensureKpiObjectSchema();
    const rows = await loadKpiTargetAllocationRowsForUi({
      allocationId: Number(kpiObjectId),
      responsibleId
    });

    if (!rows.length) {
      return res.status(404).json({ error: "KPI target allocation not found" });
    }

    res.json(rows[0]);
  } catch (error) {
    console.error("GET one KPI object error:", error);
    res.status(500).json({ error: "Failed to load KPI target allocation" });
  }
});

app.put("/api/responsibles/:responsibleId/kpi-objects/:kpiObjectId", async (req, res) => {
  const { responsibleId, kpiObjectId } = req.params;

  try {
    await ensureKpiObjectSchema();
    const responsiblePeopleContext = await loadPeopleContextByPeopleId(responsibleId);
    const normalizedResponsiblePeopleId =
      responsiblePeopleContext?.people_id ||
      normalizeOptionalIntegerInput(responsibleId);
    const {
      kpi_id,
      plant_id,
      zone_id,
      unit_id,
      function: functionName,
      product_id,
      product_line_id,
      market_id,
      customer_id,
      target_value,
      target_unit,
      local_currency,
      kpi_type,
      target_setup_date,
      target_start_date,
      target_end_date,
      last_best_target,
      previous_target_value,
      target_status,
      set_by_people_id,
      approved_by_people_id,
      comments
    } = prepareKpiObjectPayload({
      ...req.body,
      set_by_people_id: req.body.set_by_people_id || normalizedResponsiblePeopleId
    });

    const result = await pool.query(
      `
      UPDATE public.kpi_target_allocation
      SET
        kpi_id = $1,
        plant_id = $2,
        zone_id = $3,
        unit_id = $4,
        "function" = $5,
        product_id = $6,
        product_line_id = $7,
        market_id = $8,
        customer_id = $9,
        target_value = $10,
        target_unit = $11,
        local_currency = $12,
        kpi_type = $13,
        target_setup_date = $14,
        target_start_date = $15,
        target_end_date = $16,
        last_best_target = $17,
        previous_target_value = $18,
        target_status = $19,
        set_by_people_id = $20,
        approved_by_people_id = $21,
        comments = $22,
        updated_at = NOW()
      WHERE kpi_target_allocation_id = $23
      RETURNING *
      `,
      [
        kpi_id,
        plant_id,
        zone_id,
        unit_id,
        functionName,
        product_id,
        product_line_id,
        market_id,
        customer_id,
        target_value,
        target_unit,
        local_currency,
        kpi_type,
        target_setup_date,
        target_start_date,
        target_end_date,
        last_best_target,
        previous_target_value,
        target_status,
        set_by_people_id,
        approved_by_people_id,
        comments,
        kpiObjectId,
      ]
    );

    if (!result.rows.length) {
      return res.status(404).json({ error: "KPI target allocation not found" });
    }

    const rows = await loadKpiTargetAllocationRowsForUi({
      allocationId: Number(kpiObjectId),
      responsibleId: normalizedResponsiblePeopleId || responsibleId
    });

    res.json(rows[0] || result.rows[0]);
  } catch (error) {
    console.error("PUT KPI object error:", error);
    res.status(error.statusCode || 500).json({
      error: error.statusCode ? error.message : "Failed to update KPI target allocation"
    });
  }
});

app.delete("/api/responsibles/:responsibleId/kpi-objects/:kpiObjectId", async (req, res) => {
  const { kpiObjectId } = req.params;

  try {
    await ensureKpiObjectSchema();
    const result = await pool.query(
      `
      DELETE FROM public.kpi_target_allocation
      WHERE kpi_target_allocation_id = $1
      RETURNING kpi_target_allocation_id
      `,
      [kpiObjectId]
    );

    if (!result.rows.length) {
      return res.status(404).json({ error: "KPI target allocation not found" });
    }

    res.json({ success: true });
  } catch (error) {
    console.error("DELETE KPI object error:", error);
    res.status(500).json({ error: "Failed to delete KPI target allocation" });
  }
});

app.put("/api/responsibles/:responsibleId/parameter-kpis/:parameterKpiId", async (req, res) => {
  const { responsibleId, parameterKpiId } = req.params;

  try {
    await ensureResponsibleParameterKpiSchema();

    const {
      kpi_id,
      location,
      local_currency,
      value,
      value_date,
      target,
      best_new_target,
      kpi_type
    } = prepareResponsibleParameterKpiPayload(req.body);

    const result = await pool.query(
      `
      UPDATE public.responsible_parameter_kpis
      SET
        kpi_id = $1,
        location = $2,
        local_currency = $3,
        value = $4,
        value_date = $5,
        target = $6,
        best_new_target = $7,
        kpi_type = $8,
        updated_at = NOW()
      WHERE parameter_kpi_id = $9
        AND responsible_id = $10
      RETURNING *
      `,
      [
        kpi_id,
        location,
        local_currency,
        value,
        value_date,
        target,
        best_new_target,
        kpi_type,
        parameterKpiId,
        responsibleId
      ]
    );

    if (!result.rows.length) {
      return res.status(404).json({ error: "Parameter KPI not found" });
    }

    res.json(result.rows[0]);
  } catch (error) {
    console.error("PUT parameter KPI error:", error);
    res.status(error.statusCode || 500).json({
      error: error.statusCode ? error.message : "Failed to update parameter KPI"
    });
  }
});

app.delete("/api/responsibles/:responsibleId/parameter-kpis/:parameterKpiId", async (req, res) => {
  const { responsibleId, parameterKpiId } = req.params;

  try {
    await ensureResponsibleParameterKpiSchema();

    const result = await pool.query(
      `
      DELETE FROM public.responsible_parameter_kpis
      WHERE parameter_kpi_id = $1
        AND responsible_id = $2
      RETURNING parameter_kpi_id
      `,
      [parameterKpiId, responsibleId]
    );

    if (!result.rows.length) {
      return res.status(404).json({ error: "Parameter KPI not found" });
    }

    res.json({ success: true });
  } catch (error) {
    console.error("DELETE parameter KPI error:", error);
    res.status(500).json({ error: "Failed to delete parameter KPI" });
  }
});


app.get("/api/responsibles/:responsibleId/kpi-history-monthly", async (req, res) => {
  const { responsibleId } = req.params;

  try {
    const result = await pool.query(
      `
      WITH ranked AS (
        SELECT
          h.kpi_id,
          k.indicator_title,
          k.indicator_sub_title,
          k.high_limit,
          k.low_limit,
          k.target,
          date_trunc('month', h.updated_at)::date AS month_start,
          to_char(date_trunc('month', h.updated_at), 'Mon YYYY') AS month_label,
          COALESCE(NULLIF(h.new_value, ''), '0')::numeric AS new_value_num,
          h.updated_at,
          ROW_NUMBER() OVER (
            PARTITION BY h.kpi_id, date_trunc('month', h.updated_at)
            ORDER BY h.updated_at DESC
          ) AS rn
        FROM public.kpi_values_hist26 h
        JOIN public."Kpi" k
          ON k.kpi_id = h.kpi_id
        WHERE h.responsible_id = $1
      )
      SELECT
        kpi_id,
        indicator_title,
        indicator_sub_title,
        high_limit,
        low_limit,
        target,
        month_start,
        month_label,
        new_value_num
      FROM ranked
      WHERE rn = 1
      ORDER BY kpi_id, month_start
      `,
      [responsibleId]
    );

    const grouped = {};

    for (const row of result.rows) {
      if (!grouped[row.kpi_id]) {
        grouped[row.kpi_id] = {
          kpi_id: row.kpi_id,
          indicator_title: row.indicator_title,
          indicator_sub_title: row.indicator_sub_title,
          labels: [],
          values: [],
          targetValue: row.target !== null && row.target !== "" ? Number(row.target) : null,
          highLimitValue: row.high_limit !== null ? Number(row.high_limit) : null,
          lowLimitValue: row.low_limit !== null ? Number(row.low_limit) : null
        };
      }

      grouped[row.kpi_id].labels.push(row.month_label);
      grouped[row.kpi_id].values.push(Number(row.new_value_num) || 0);
    }

    const finalRows = Object.values(grouped).map(item => ({
      kpi_id: item.kpi_id,
      indicator_title: item.indicator_title,
      indicator_sub_title: item.indicator_sub_title,
      labels: item.labels,
      values: item.values,
      targetValue: item.targetValue,
      highLimitValue: item.highLimitValue,
      lowLimitValue: item.lowLimitValue
    }));

    res.json(finalRows);
  } catch (error) {
    console.error("GET monthly KPI history error:", error);
    res.status(500).json({ error: "Failed to load monthly KPI history" });
  }
});

    
app.get("/responsible/:responsibleId/dashboard", async (req, res) => {
  const { responsibleId } = req.params;
  const [
    responsibleContext,
    subjectHierarchyData,
    roleOptions,
    allocationLookups
  ] = await Promise.all([
    loadFormResponsibleContext(responsibleId),
    loadSubjectHierarchyData(),
    loadRoleOptions(),
    loadKpiTargetAllocationLookups()
  ]);

  const responsibleName = responsibleContext?.name || "KPI Workspace";
  const dashboardResponsibleId = responsibleContext?.people_id || responsibleId;
  const kpiSubjectHierarchy = subjectHierarchyData.tree;

  res.send(`
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Responsible KPI Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

    <style>
      * {
        box-sizing: border-box;
        margin: 0;
        padding: 0;
      }

      :root {
        --bg: #f4f7fb;
        --bg-2: #eef4ff;
        --sidebar: #0f172a;
        --sidebar-2: #111c33;
        --card: rgba(255, 255, 255, 0.92);
        --card-solid: #ffffff;
        --line: rgba(15, 23, 42, 0.08);
        --line-strong: rgba(15, 23, 42, 0.12);
        --text: #0f172a;
        --muted: #64748b;
        --muted-2: #94a3b8;
        --primary: #2563eb;
        --primary-2: #4f46e5;
        --cyan: #06b6d4;
        --success: #10b981;
        --warning: #f59e0b;
        --danger: #ef4444;
        --soft-blue: rgba(37,99,235,0.08);
        --soft-red: rgba(239,68,68,0.08);
        --soft-amber: rgba(245,158,11,0.10);
        --soft-slate: #f8fbff;
        --shadow-sm: 0 8px 20px rgba(15, 23, 42, 0.05);
        --shadow-md: 0 18px 40px rgba(15, 23, 42, 0.08);
        --shadow-lg: 0 28px 70px rgba(15, 23, 42, 0.10);
        --radius-xl: 28px;
        --radius-lg: 22px;
        --radius-md: 16px;
        --radius-sm: 12px;
      }

      body {
        font-family: "Inter", sans-serif;
        background:
          radial-gradient(circle at top left, rgba(79,70,229,0.08), transparent 18%),
          radial-gradient(circle at top right, rgba(6,182,212,0.08), transparent 18%),
          linear-gradient(180deg, #f8fbff 0%, #f3f7fc 100%);
        color: var(--text);
        min-height: 100vh;
      }

    
      .layout {
        display: grid;
        grid-template-columns: 280px 1fr;
        min-height: 100vh;
      }

      .sidebar {
        position: sticky;
        top: 0;
        height: 100vh;
        background:
          radial-gradient(circle at top right, rgba(59,130,246,0.16), transparent 28%),
          linear-gradient(180deg, var(--sidebar) 0%, var(--sidebar-2) 100%);
        color: white;
        padding: 24px 18px;
        border-right: 1px solid rgba(255,255,255,0.06);
        display: flex;
        flex-direction: column;
        gap: 24px;
      }

      .brand {
        display: flex;
        align-items: center;
        gap: 14px;
        padding: 10px 12px;
        border-radius: 18px;
        background: rgba(255,255,255,0.06);
        border: 1px solid rgba(255,255,255,0.08);
      }

      .brand-badge {
        width: 42px;
        height: 42px;
        border-radius: 14px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: linear-gradient(135deg, var(--primary-2), var(--cyan));
        font-weight: 900;
        font-size: 18px;
        box-shadow: 0 14px 28px rgba(37,99,235,0.28);
      }

      .brand h2 {
        font-size: 18px;
        font-weight: 900;
        letter-spacing: -0.02em;
      }

      .brand p {
        margin-top: 4px;
        font-size: 12px;
        color: rgba(255,255,255,0.68);
      }

      .sidebar-section-title {
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.14em;
        color: rgba(255,255,255,0.45);
        font-weight: 800;
        margin: 6px 12px 0;
      }

      .nav {
        display: flex;
        flex-direction: column;
        gap: 8px;
      }

      .nav-item {
        display: flex;
        align-items: center;
        gap: 12px;
        color: rgba(255,255,255,0.82);
        text-decoration: none;
        padding: 14px;
        border-radius: 16px;
        transition: 0.22s ease;
        font-weight: 700;
        border: 1px solid transparent;
      }

      .nav-item:hover,
      .nav-item.active {
        background: rgba(255,255,255,0.08);
        border-color: rgba(255,255,255,0.08);
        color: white;
        transform: translateX(2px);
      }

      .nav-icon {
        width: 34px;
        height: 34px;
        border-radius: 10px;
        background: rgba(255,255,255,0.08);
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 16px;
      }

      .sidebar-footer {
        margin-top: auto;
        background: rgba(255,255,255,0.06);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 20px;
        padding: 18px;
      }

      .sidebar-footer h4 {
        font-size: 14px;
        font-weight: 800;
      }

      .sidebar-footer p {
        margin-top: 8px;
        font-size: 13px;
        color: rgba(255,255,255,0.7);
        line-height: 1.6;
      }

      .content {
        padding: 24px;
      }

      .topbar {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 16px;
        margin-bottom: 22px;
        flex-wrap: wrap;
      }

      .topbar-left h1 {
        font-size: 34px;
        font-weight: 900;
        letter-spacing: -1.2px;
        color: #0b1220;
      }

      .topbar-left p {
        margin-top: 8px;
        color: var(--muted);
        font-size: 14px;
      }

      .topbar-right {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
      }

      .btn {
        border: none;
        cursor: pointer;
        border-radius: 14px;
        padding: 12px 16px;
        font-size: 14px;
        font-weight: 800;
        transition: all 0.22s ease;
      }

      .btn:hover {
        transform: translateY(-2px);
      }

      .btn-primary {
        color: white;
        background: linear-gradient(135deg, var(--primary-2), var(--primary), var(--cyan));
        box-shadow: 0 14px 28px rgba(37,99,235,0.18);
      }

      .btn-soft {
        color: var(--text);
        background: rgba(255,255,255,0.92);
        border: 1px solid var(--line-strong);
        box-shadow: var(--shadow-sm);
      }

      .btn-danger {
        color: white;
        background: linear-gradient(135deg, #f87171, #ef4444);
        box-shadow: 0 12px 22px rgba(239,68,68,0.16);
      }

      .hero-panel {
        position: relative;
        overflow: hidden;
        background: linear-gradient(135deg, #ffffff 0%, #f7faff 100%);
        border: 1px solid var(--line);
        border-radius: var(--radius-xl);
        box-shadow: var(--shadow-lg);
        padding: 24px;
        margin-bottom: 22px;
      }

      .hero-panel::before {
        content: "";
        position: absolute;
        width: 240px;
        height: 240px;
        border-radius: 50%;
        background: radial-gradient(circle, rgba(37,99,235,0.08), transparent 70%);
        top: -120px;
        right: -80px;
      }

      .hero-panel::after {
        content: "";
        position: absolute;
        width: 220px;
        height: 220px;
        border-radius: 50%;
        background: radial-gradient(circle, rgba(6,182,212,0.08), transparent 70%);
        bottom: -120px;
        left: -60px;
      }

      .hero-row {
        position: relative;
        z-index: 1;
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 20px;
        flex-wrap: wrap;
      }

      .hero-meta h3 {
        font-size: 22px;
        font-weight: 900;
      }

      .hero-meta p {
        margin-top: 8px;
        color: var(--muted);
        font-size: 14px;
      }

      .responsible-chip {
        margin-top: 12px;
        display: inline-flex;
        align-items: center;
        gap: 8px;
        background: rgba(37,99,235,0.08);
        color: #1d4ed8;
        padding: 8px 14px;
        border-radius: 999px;
        font-weight: 800;
        font-size: 14px;
      }

      .hero-badge {
        display: inline-flex;
        align-items: center;
        gap: 10px;
        padding: 12px 16px;
        border-radius: 999px;
        background: rgba(255,255,255,0.90);
        border: 1px solid var(--line);
        box-shadow: var(--shadow-sm);
        font-weight: 800;
      }

      .stats-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 16px;
        margin-bottom: 22px;
      }

      .stat-card {
        position: relative;
        overflow: hidden;
        background: rgba(255,255,255,0.9);
        border: 1px solid rgba(255,255,255,0.8);
        border-radius: var(--radius-lg);
        padding: 20px;
        box-shadow: var(--shadow-md);
      }

      .stat-card::before {
        content: "";
        position: absolute;
        top: -20px;
        right: -20px;
        width: 100px;
        height: 100px;
        border-radius: 50%;
        background: linear-gradient(135deg, rgba(79,70,229,0.10), rgba(6,182,212,0.08));
      }

      .stat-label {
        color: var(--muted);
        font-size: 13px;
        font-weight: 700;
        margin-bottom: 10px;
      }

      .stat-value {
        font-size: 28px;
        font-weight: 900;
        letter-spacing: -1px;
      }

      .toolbar {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 14px;
        flex-wrap: wrap;
        margin-bottom: 20px;
      }

      .search-wrap {
        flex: 1;
        min-width: 280px;
      }

      .search {
        width: 100%;
        border: 1px solid rgba(148,163,184,0.25);
        background: rgba(255,255,255,0.95);
        color: var(--text);
        border-radius: 18px;
        padding: 15px 18px;
        font-size: 14px;
        outline: none;
        box-shadow: var(--shadow-sm);
        transition: all 0.18s ease;
      }

      .search::placeholder {
        color: #94a3b8;
      }

      .search:focus {
        border-color: rgba(37,99,235,0.35);
        box-shadow: 0 0 0 4px rgba(37,99,235,0.10);
      }

      .section-head {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        margin-bottom: 18px;
      }

      .section-head h2 {
        font-size: 20px;
        font-weight: 900;
      }

      .section-head p {
        color: var(--muted);
        font-size: 13px;
        margin-top: 4px;
      }

      .section-head.section-head--nested {
        margin: 0 0 14px;
      }

      .section-head.section-head--nested h2 {
        font-size: 17px;
        font-weight: 900;
      }

      .section-head.section-head--nested p {
        font-size: 12px;
      }

      .grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0,1fr));
        gap: 16px;
      }

      .kpi-table-shell {
        grid-column: 1 / -1;
        background: rgba(255,255,255,0.94);
        border: 1px solid rgba(15,23,42,0.06);
        border-radius: 24px;
        box-shadow: var(--shadow-md);
        overflow: hidden;
      }

      .kpi-table-wrap {
        overflow-x: auto;
      }

      .kpi-table {
        width: 100%;
        min-width: 640px;
        border-collapse: collapse;
      }

      .kpi-table thead tr {
        background: linear-gradient(180deg, #f8fbff 0%, #eef4fb 100%);
      }

      .kpi-table th {
        padding: 16px 18px;
        text-align: left;
        font-size: 11px;
        font-weight: 900;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: #64748b;
        border-bottom: 1px solid #e2e8f0;
      }

      .kpi-table td {
        padding: 18px;
        border-bottom: 1px solid #e2e8f0;
        vertical-align: middle;
        background: rgba(255,255,255,0.96);
      }

      .kpi-table tbody tr:last-child td {
        border-bottom: none;
      }

      .kpi-table tbody tr:hover td {
        background: #f8fbff;
      }

      .kpi-row-title {
        font-size: 15px;
        font-weight: 900;
        color: #0f172a;
        line-height: 1.35;
      }

      .kpi-row-subtitle {
        font-size: 14px;
        color: #475569;
        line-height: 1.5;
      }

      .kpi-table-actions {
        width: 140px;
        text-align: right;
        white-space: nowrap;
      }

      .table-actions-group {
        display: inline-flex;
        align-items: center;
        justify-content: flex-end;
        gap: 8px;
        flex-wrap: wrap;
      }

      .parameter-section {
        margin-top: 24px;
      }

      .parameter-section-shell {
        background: rgba(255,255,255,0.78);
        border: 1px solid rgba(15,23,42,0.05);
        border-radius: 26px;
        box-shadow: var(--shadow-md);
        padding: 20px;
      }

      .allocation-table .kpi-table-actions {
        width: 190px;
      }

      .allocation-row-meta {
        margin-top: 6px;
        font-size: 12px;
        color: #64748b;
        line-height: 1.5;
      }

      .allocation-target-value {
        font-size: 15px;
        font-weight: 900;
        color: #0f172a;
        line-height: 1.35;
      }

      .parameter-table-shell {
        grid-column: 1 / -1;
        background: rgba(255,255,255,0.95);
        border: 1px solid rgba(15,23,42,0.06);
        border-radius: 24px;
        box-shadow: var(--shadow-md);
        overflow: hidden;
      }

      .parameter-table-wrap {
        overflow-x: auto;
      }

      .parameter-table {
        width: 100%;
        min-width: 1120px;
        border-collapse: collapse;
      }

      .parameter-table thead tr {
        background:
          radial-gradient(circle at top right, rgba(6,182,212,0.09), transparent 36%),
          linear-gradient(180deg, #f8fbff 0%, #eef5ff 100%);
      }

      .parameter-table th {
        padding: 16px 18px;
        text-align: left;
        font-size: 11px;
        font-weight: 900;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: #64748b;
        border-bottom: 1px solid #e2e8f0;
      }

      .parameter-table td {
        padding: 16px 18px;
        border-bottom: 1px solid #e2e8f0;
        background: rgba(255,255,255,0.98);
        color: #0f172a;
        vertical-align: middle;
      }

      .parameter-table tbody tr:last-child td {
        border-bottom: none;
      }

      .parameter-table tbody tr:hover td {
        background: #f8fbff;
      }

      .parameter-table-title {
        display: inline-flex;
        align-items: center;
        padding: 8px 12px;
        border-radius: 999px;
        background: rgba(79,70,229,0.10);
        color: #4338ca;
        font-size: 12px;
        font-weight: 900;
        letter-spacing: 0.04em;
      }

      .parameter-table-subvalue {
        font-size: 13px;
        font-weight: 800;
        color: #0f172a;
      }

      .parameter-table-meta {
        font-size: 12px;
        color: #64748b;
        margin-top: 4px;
      }

      .parameter-table-actions {
        width: 140px;
        text-align: right;
        white-space: nowrap;
      }

      .parameter-pill {
        display: inline-flex;
        align-items: center;
        padding: 6px 10px;
        border-radius: 999px;
        background: rgba(16,185,129,0.10);
        color: #047857;
        font-size: 11px;
        font-weight: 900;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }

      .parameter-modal .modal-overview {
        grid-template-columns: repeat(4, minmax(0, 1fr));
      }

      .readonly-field {
        background: #f8fafc !important;
        color: #475569 !important;
        border-color: #dbe4f0 !important;
      }

      .action-btn {
        border: none;
        border-radius: 12px;
        padding: 10px 13px;
        font-size: 12px;
        font-weight: 800;
        cursor: pointer;
        transition: 0.18s ease;
      }

      .action-btn:hover {
        transform: translateY(-1px);
      }

      .edit-btn {
        background: rgba(37,99,235,0.08);
        color: #1d4ed8;
        border: 1px solid rgba(37,99,235,0.10);
      }

      .delete-btn {
        background: rgba(239,68,68,0.08);
        color: #dc2626;
        border: 1px solid rgba(239,68,68,0.10);
      }

      .empty {
        grid-column: 1 / -1;
        background: rgba(255,255,255,0.92);
        border: 1px dashed #dbe5f0;
        border-radius: 24px;
        padding: 60px 20px;
        text-align: center;
        color: var(--muted);
        box-shadow: var(--shadow-md);
      }

      .modal-backdrop {
        position: fixed;
        inset: 0;
        background: rgba(15,23,42,0.34);
        backdrop-filter: blur(8px);
        display: none;
        align-items: flex-start;
        justify-content: center;
        padding: 20px;
        z-index: 999;
        overflow: auto;
      }

      .modal-backdrop.open {
        display: flex;
      }

      .modal {
        width: min(1360px, calc(100vw - 24px));
        max-width: none;
        max-height: none;
        overflow: visible;
        background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
        border-radius: 30px;
        box-shadow: 0 32px 90px rgba(15, 23, 42, 0.20);
        border: 1px solid rgba(255,255,255,0.9);
        display: flex;
        flex-direction: column;
        margin: auto;
      }

   #modalBackdrop {
     align-items: center;
     padding: 8px;
     overflow: auto;
    }


.kpi-attributes-modal {
  width: min(1900px, calc(100vw - 12px));
  height: calc(100vh - 12px);
  overflow: hidden;
  display: flex;
  flex-direction: column;
}

      .modal-header {
        padding: 22px 24px 18px;
        border-bottom: 1px solid rgba(15,23,42,0.06);
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 16px;
        background: rgba(255,255,255,0.94);
        backdrop-filter: blur(12px);
        position: sticky;
        top: 0;
        z-index: 5;
      }

      .modal-title-wrap {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }

      .modal-header h2 {
        font-size: 28px;
        font-weight: 900;
        color: #0f172a;
        letter-spacing: -0.03em;
      }

      .modal-subtitle {
        font-size: 14px;
        color: #64748b;
        line-height: 1.5;
        max-width: 700px;
      }

    .modal-body {
        padding: 8px 20px 6px;
        overflow: visible;
        display: flex;
        flex-direction: column;
        gap: 4px;
        background:
          radial-gradient(circle at top right, rgba(37,99,235,0.04), transparent 22%),
          linear-gradient(180deg, #fbfdff 0%, #f8fbff 100%);
      }

     .modal-overview {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 8px;
        margin-bottom: 0;
        flex-shrink: 0;
      }

      .overview-card {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 8px 12px;
      }

      .overview-card .label {
        font-size: 11px;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #64748b;
      }

      .overview-card .value {
        margin-top: 6px;
        font-size: 14px;
        font-weight: 800;
        color: #0f172a;
      }

     .form-section {
        background: #fafbfc;
        border: 1px solid #e5e9f0;
        border-radius: 14px;
        padding: 10px 14px 11px;
        margin-bottom: 0;
        flex-shrink: 0;
      }

      .section-title {
        margin: 0 0 8px;
        font-size: 10px;
        font-weight: 900;
        color: #4f46e5;
        text-transform: uppercase;
        letter-spacing: 0.14em;
      }

      .section-subtitle {
        font-size: 11px;
        color: #64748b;
        margin-bottom: 10px;
      }

      .form-grid {
        display: grid;
        grid-template-columns: repeat(18, minmax(0, 1fr));
        gap: 8px;
        align-items: end;
      }

      .field {
        display: flex;
        flex-direction: column;
        gap: 4px;
      }

      .field.col-2 { grid-column: span 2; }
      .field.col-3 { grid-column: span 3; }
      .field.col-4 { grid-column: span 4; }
      .field.col-5 { grid-column: span 5; }
      .field.col-6 { grid-column: span 6; }
      .field.col-8 { grid-column: span 8; }
      .field.col-12 { grid-column: 1 / -1; }

   .field label {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
        font-size: 10px;
        font-weight: 800;
        color: #334155;
      }

      .field .hint {
        font-size: 9px;
        font-weight: 700;
        color: #94a3b8;
      }

      .field input,
      .field textarea,
      .field select {
        width: 100%;
        border: 1px solid rgba(148,163,184,0.28);
        background: #ffffff;
        color: #0f172a;
        border-radius: 10px;
        padding: 7px 10px;
        font-size: 13px;
        font-family: inherit;
        outline: none;
        transition: all 0.15s ease;
      }

      .field input::placeholder,
      .field textarea::placeholder,
      .field select::placeholder {
        color: #94a3b8;
      }

      .field input:focus,
      .field textarea:focus,
      .field select:focus {
        border-color: rgba(37,99,235,0.34);
        box-shadow: 0 0 0 4px rgba(37,99,235,0.10);
      }

      .field textarea {
        min-height: 56px;
        resize: none;
        line-height: 1.45;
      }

      .field.is-hidden {
        display: none;
      }

      .tree-select {
        position: relative;
        width: 100%;
      }

      .tree-select-trigger {
        width: 100%;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 10px;
        border: 1px solid rgba(148,163,184,0.28);
        background: #ffffff;
        color: #0f172a;
        border-radius: 10px;
        padding: 9px 10px;
        font-size: 13px;
        font-family: inherit;
        text-align: left;
        cursor: pointer;
        transition: all 0.15s ease;
      }

      .hierarchy-tree-field {
        grid-column: 1 / -1;
      }

      .hierarchy-tree-field .tree-select-trigger {
        min-height: 78px;
        padding: 14px 16px;
        border-radius: 18px;
        background:
          radial-gradient(circle at top right, rgba(37,99,235,0.10), transparent 32%),
          linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
        box-shadow: 0 12px 28px rgba(15,23,42,0.06);
      }

      .hierarchy-tree-field .tree-select-panel {
        display: block;
        margin-top: 14px;
      }

      .hierarchy-tree-field .tree-select-chevron {
        display: none;
      }

      .tree-select-trigger:hover,
      .tree-select.open .tree-select-trigger {
        border-color: rgba(37,99,235,0.34);
        box-shadow: 0 0 0 4px rgba(37,99,235,0.10);
      }

      .tree-select-value {
        min-width: 0;
        display: flex;
        flex-direction: column;
        gap: 2px;
      }

      .tree-select-label {
        font-size: 13px;
        font-weight: 700;
        color: #0f172a;
        line-height: 1.35;
      }

      .tree-select-meta {
        font-size: 11px;
        color: #94a3b8;
        line-height: 1.35;
      }

      .tree-select-chevron {
        font-size: 12px;
        color: #64748b;
        flex-shrink: 0;
      }

      .tree-select-panel {
        display: none;
        margin-top: 8px;
        border: 1px solid rgba(148,163,184,0.22);
        border-radius: 18px;
        background:
          radial-gradient(circle at top right, rgba(37,99,235,0.08), transparent 30%),
          linear-gradient(180deg, #ffffff 0%, #f7fbff 100%);
        box-shadow: 0 22px 48px rgba(15,23,42,0.12);
        overflow: hidden;
      }

      .tree-select.open .tree-select-panel {
        display: block;
      }

      .tree-select-toolbar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 14px;
        flex-wrap: wrap;
        padding: 12px 14px;
        border-bottom: 1px solid rgba(148,163,184,0.16);
        background: rgba(248,250,252,0.96);
      }

      .tree-select-toolbar-copy {
        min-width: min(100%, 620px);
        display: flex;
        flex-direction: column;
        gap: 10px;
      }

      .tree-select-help {
        font-size: 11px;
        color: #64748b;
        line-height: 1.45;
      }

      .tree-select-current-path {
        padding: 10px 12px;
        border-radius: 14px;
        border: 1px solid rgba(148,163,184,0.18);
        background: rgba(255,255,255,0.92);
        color: #0f172a;
        font-size: 12px;
        font-weight: 700;
        line-height: 1.5;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.85);
      }

      .tree-select-fit {
        display: inline-flex;
        align-items: center;
        padding: 6px 10px;
        border-radius: 999px;
        background: rgba(37,99,235,0.08);
        color: #2563eb;
        font-size: 10px;
        font-weight: 900;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        white-space: nowrap;
      }

    .tree-list {
         height: auto;
         max-height: 60vh;
        overflow: auto;
        padding: 10px;
        background: linear-gradient(180deg, rgba(255,255,255,0.99) 0%, rgba(248,251,255,0.99) 100%);
     }

    .tree-list:active {
      cursor: grabbing;
     }

    .tree-list::-webkit-scrollbar {
         width: 5px;
     } 

    .tree-list::-webkit-scrollbar-track {
     background: transparent;
    }

 .tree-list::-webkit-scrollbar-thumb {
    background: rgba(148,163,184,0.35);
    border-radius: 99px;
 }

 .v-tree-node {
  display: flex;
  flex-direction: column;
}

.v-tree-row {
  display: flex;
  align-items: center;
  gap: 0;
  min-height: 38px;
  border-radius: 10px;
  cursor: pointer;
  transition: background 0.14s ease;
  position: relative;
  margin-bottom: 2px;
}

.v-tree-row:hover {
  background: rgba(37,99,235,0.06);
}

.v-tree-row.selected {
  background: linear-gradient(135deg, rgba(37,99,235,0.12), rgba(6,182,212,0.08));
  border: 1px solid rgba(37,99,235,0.18);
}

.v-tree-indent {
  flex-shrink: 0;
  display: flex;
  align-items: stretch;
}

.v-tree-indent-segment {
  width: 20px;
  flex-shrink: 0;
  position: relative;
}

.v-tree-indent-segment.has-line::before {
  content: "";
  position: absolute;
  left: 9px;
  top: 0;
  bottom: 0;
  width: 1px;
  background: rgba(148,163,184,0.28);
}

.v-tree-toggle-btn {
  width: 22px;
  height: 22px;
  flex-shrink: 0;
  border: none;
  background: rgba(37,99,235,0.08);
  border-radius: 6px;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  color: #2563eb;
  font-size: 10px;
  transition: all 0.15s ease;
  margin-right: 6px;
}

.v-tree-toggle-btn:hover {
  background: rgba(37,99,235,0.16);
}

.v-tree-toggle-placeholder {
  width: 22px;
  height: 22px;
  flex-shrink: 0;
  margin-right: 6px;
}

.v-tree-icon {
  font-size: 13px;
  margin-right: 7px;
  flex-shrink: 0;
  opacity: 0.75;
}

.v-tree-content {
  flex: 1;
  min-width: 0;
  padding: 6px 10px 6px 0;
}

.v-tree-name {
  font-size: 13px;
  font-weight: 700;
  color: #0f172a;
  line-height: 1.3;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.v-tree-row.selected .v-tree-name {
  color: #1d4ed8;
}

.v-tree-path {
  font-size: 11px;
  color: #94a3b8;
  margin-top: 1px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.v-tree-level-badge {
  flex-shrink: 0;
  font-size: 9px;
  font-weight: 900;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  padding: 2px 6px;
  border-radius: 99px;
  margin-right: 8px;
  background: rgba(148,163,184,0.12);
  color: #94a3b8;
}

.v-tree-level-badge.l0 { background: rgba(79,70,229,0.10); color: #4338ca; }
.v-tree-level-badge.l1 { background: rgba(37,99,235,0.10); color: #1d4ed8; }
.v-tree-level-badge.l2 { background: rgba(6,182,212,0.10); color: #0891b2; }
.v-tree-level-badge.l3 { background: rgba(16,185,129,0.10); color: #047857; }

.v-tree-children {
  display: flex;
  flex-direction: column;
}

.v-tree-children.collapsed {
  display: none;
}

.v-tree-search {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 14px;
  border-bottom: 1px solid rgba(148,163,184,0.14);
}

.v-tree-search input {
  flex: 1;
  border: 1px solid rgba(148,163,184,0.24);
  border-radius: 8px;
  padding: 7px 10px;
  font-size: 12px;
  font-family: inherit;
  color: #0f172a;
  outline: none;
  background: #fff;
}

.v-tree-search input:focus {
  border-color: rgba(37,99,235,0.34);
  box-shadow: 0 0 0 3px rgba(37,99,235,0.10);
}

.v-tree-actions {
  display: flex;
  gap: 6px;
}

.v-tree-action-btn {
  border: none;
  background: rgba(37,99,235,0.08);
  color: #2563eb;
  border-radius: 7px;
  padding: 5px 9px;
  font-size: 11px;
  font-weight: 800;
  cursor: pointer;
  white-space: nowrap;
  transition: background 0.14s;
}

.v-tree-action-btn:hover {
  background: rgba(37,99,235,0.16);
}

.v-tree-empty {
  padding: 28px 16px;
  text-align: center;
  color: #94a3b8;
  font-size: 13px;
}


.tree-zoom-actions {
  display: flex;
  align-items: center;
  gap: 6px;
  background: rgba(37,99,235,0.08);
  padding: 6px;
  border-radius: 999px;
}

.tree-zoom-actions button {
  border: none;
  background: #fff;
  color: #2563eb;
  font-weight: 900;
  border-radius: 999px;
  padding: 5px 10px;
  cursor: pointer;
}

.tree-zoom-actions span {
  min-width: 46px;
  text-align: center;
  font-size: 11px;
  font-weight: 900;
  color: #2563eb;
}

.subject-tree-stage {
  min-width: max-content;
  min-height: max-content;
}

.subject-tree-scale-layer {
  transform: scale(var(--subject-tree-scale, 1));
  transform-origin: top left;
  width: max-content;
}

.subject-tree-root,
.subject-tree-children {
  list-style: none;
  padding-left: 28px;
  margin: 0;
  position: relative;
}

.subject-tree-root {
  padding-left: 0;
}

.subject-tree-item {
  position: relative;
  padding: 8px 0 8px 28px;
}

.subject-tree-item::before {
  content: "";
  position: absolute;
  top: 28px;
  left: 0;
  width: 24px;
  height: 1px;
  background: rgba(37,99,235,0.28);
}

.subject-tree-children::before {
  content: "";
  position: absolute;
  top: 0;
  left: 12px;
  bottom: 18px;
  width: 1px;
  background: rgba(37,99,235,0.22);
}

.subject-tree-node-shell {
  display: flex;
  align-items: center;
  gap: 8px;
}

.subject-tree-node-btn {
  min-width: 220px;
  max-width: 320px;
  border: 1px solid rgba(148,163,184,0.25);
  background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
  border-radius: 16px;
  padding: 12px 14px;
  text-align: left;
  box-shadow: 0 10px 24px rgba(15,23,42,0.06);
  cursor: pointer;
}

.subject-tree-node-btn:hover {
  border-color: rgba(37,99,235,0.35);
  box-shadow: 0 0 0 4px rgba(37,99,235,0.10);
}

.subject-tree-node-btn.selected {
  border-color: rgba(37,99,235,0.45);
  background: linear-gradient(135deg, rgba(37,99,235,0.14), rgba(6,182,212,0.08));
}

.subject-tree-node-kicker {
  display: inline-flex;
  font-size: 9px;
  font-weight: 900;
  color: #2563eb;
  background: rgba(37,99,235,0.08);
  padding: 3px 7px;
  border-radius: 999px;
  margin-bottom: 6px;
}

.subject-tree-node-name {
  display: block;
  font-size: 13px;
  font-weight: 800;
  color: #0f172a;
}

.subject-tree-node-path {
  display: block;
  margin-top: 3px;
  font-size: 11px;
  color: #94a3b8;
}

.subject-tree-toggle {
  border: none;
  background: rgba(37,99,235,0.08);
  color: #2563eb;
  border-radius: 999px;
  padding: 7px 10px;
  font-size: 10px;
  font-weight: 900;
  cursor: pointer;
}

    .subject-tree-toggle.placeholder {
        background: rgba(148,163,184,0.10);
       color: #94a3b8;
       }
      .field-help {
        font-size: 12px;
        color: #64748b;
        line-height: 1.45;
      }

      .parameter-kpi-summary {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 10px;
        padding: 12px 14px;
        border-radius: 14px;
        border: 1px solid rgba(148,163,184,0.2);
        background: linear-gradient(135deg, #ffffff 0%, #f8fbff 100%);
      }

      .parameter-kpi-summary-item {
        min-width: 0;
      }

      .parameter-kpi-summary-label {
        font-size: 10px;
        font-weight: 900;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #94a3b8;
      }

      .parameter-kpi-summary-value {
        margin-top: 6px;
        font-size: 13px;
        font-weight: 800;
        color: #0f172a;
        line-height: 1.45;
        word-break: break-word;
      }

      .ratio-mode-help {
        min-height: 100%;
        padding: 10px 12px;
        border-radius: 12px;
        border: 1px solid rgba(37,99,235,0.12);
        background: linear-gradient(135deg, rgba(37,99,235,0.06), rgba(255,255,255,0.96));
      }

      .ratio-mode-help strong {
        color: #0f172a;
      }

      .readonly-input {
        background: #f3f6fb !important;
        color: #475569 !important;
        border-color: #dbe4f0 !important;
        cursor: not-allowed;
      }

      .modal-footer {
        flex-shrink: 0;
        display: flex;
        justify-content: space-between;
        gap: 12px;
        padding: 14px 20px;
        border-top: 1px solid rgba(15,23,42,0.06);
        background: rgba(255,255,255,0.97);
        backdrop-filter: blur(12px);
      }

      .footer-actions {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
      }

      .toast {
        position: fixed;
        right: 24px;
        bottom: 24px;
        background: #ffffff;
        color: #0f172a;
        padding: 14px 18px;
        border-radius: 14px;
        display: none;
        z-index: 1001;
        border: 1px solid rgba(15,23,42,0.08);
        box-shadow: var(--shadow-md);
      }

      .chart-meta {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        margin-bottom: 14px;
      }

      .chart-chip {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 8px 12px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 800;
        border: 1px solid transparent;
      }

      .chart-chip-target {
        background: rgba(239, 68, 68, 0.08);
        color: #dc2626;
        border-color: rgba(239, 68, 68, 0.12);
      }

      .chart-chip-high {
        background: rgba(245, 158, 11, 0.10);
        color: #b45309;
        border-color: rgba(245, 158, 11, 0.14);
      }

      .chart-chip-low {
        background: rgba(59, 130, 246, 0.08);
        color: #2563eb;
        border-color: rgba(59, 130, 246, 0.12);
      }

      .chart-card {
        background: rgba(255,255,255,0.92);
        border: 1px solid rgba(255,255,255,0.8);
        border-radius: 24px;
        box-shadow: var(--shadow-lg);
        padding: 22px;
      }

      .chart-card h3 {
        font-size: 20px;
        font-weight: 900;
        margin-bottom: 6px;
        color: #0f172a;
      }

      .chart-card p {
        font-size: 13px;
        color: var(--muted);
        margin-bottom: 16px;
      }

      .chart-wrap {
        height: 320px;
      }

      .kpi-inline-row {
      display: flex;
   
     }

.kpi-inline-row .field {
  flex: 1;
  min-width: 120px; /* adjust */
}

      @media (max-width: 1250px) {
        .layout {
          grid-template-columns: 90px 1fr;
        }

        .brand h2,
        .brand p,
        .nav-item span,
        .sidebar-section-title,
        .sidebar-footer {
          display: none;
        }

        .brand {
          justify-content: center;
        }

        .nav-item {
          justify-content: center;
          padding: 12px;
        }
      }

      @media (max-width: 1100px) {
        .stats-grid,
        .grid {
          grid-template-columns: repeat(2, minmax(0,1fr));
        }

        .modal-overview {
          grid-template-columns: repeat(2, minmax(0, 1fr));
        }

        .form-grid {
          grid-template-columns: repeat(12, minmax(0, 1fr));
        }

        .parameter-kpi-summary {
          grid-template-columns: repeat(2, minmax(0, 1fr));
        }
      }

      @media (max-width: 820px) {
        .layout {
          grid-template-columns: 1fr;
        }

        .sidebar {
          position: relative;
          height: auto;
          padding: 16px;
        }

        .brand h2,
        .brand p,
        .nav-item span,
        .sidebar-section-title,
        .sidebar-footer {
          display: block;
        }

        .content {
          padding: 16px;
        }

        .stats-grid,
        .grid,
        .modal-overview {
          grid-template-columns: 1fr;
        }

        .form-grid {
          grid-template-columns: 1fr;
        }

        .parameter-kpi-summary {
          grid-template-columns: 1fr;
        }

        .field.col-3,
        .field.col-4,
        .field.col-2,
        .field.col-6,
        .field.col-8,
        .field.col-12 {
          grid-column: auto;
        }

        .topbar-left h1 {
          font-size: 28px;
        }

        .modal-body {
          padding: 14px 14px 10px;
        }

        .modal-header,
        .modal-footer {
          padding-left: 16px;
          padding-right: 16px;
        }
      }


 .sf-flow {
  display: flex;
  align-items: stretch;
  gap: 0;
  min-width: max-content;
}

.sf-column {
  width: 210px;
  flex: 0 0 210px;
  display: flex;
  flex-direction: column;
  border-right: 1px solid rgba(148,163,184,0.14);
  background: #ffffff;
  animation: sfColIn 0.2s cubic-bezier(0.22,1,0.36,1);
}

.sf-column:last-child {
  border-right: none;
}

@keyframes sfColIn {
  from { opacity: 0; transform: translateX(14px); }
  to   { opacity: 1; transform: translateX(0); }
}

.sf-col-head {
  padding: 10px 13px 9px;
  border-bottom: 1px solid rgba(148,163,184,0.11);
  background: linear-gradient(180deg, #f8fbff 0%, #f2f6fd 100%);
  flex-shrink: 0;
  position: sticky;
  top: 0;
  z-index: 2;
}

.sf-col-level {
  display: block;
  font-size: 9px;
  font-weight: 900;
  letter-spacing: 0.13em;
  text-transform: uppercase;
  color: #94a3b8;
  margin-bottom: 2px;
}

.sf-col-title {
  display: block;
  font-size: 11px;
  font-weight: 900;
  color: #4338ca;
  letter-spacing: -0.01em;
}

.sf-col-count {
  display: block;
  margin-top: 2px;
  font-size: 10px;
  color: #94a3b8;
  font-weight: 700;
}

.sf-col-body {
  flex: 1;
  overflow-y: auto;
  padding: 5px;
}

.sf-col-body::-webkit-scrollbar { width: 3px; }
.sf-col-body::-webkit-scrollbar-track { background: transparent; }
.sf-col-body::-webkit-scrollbar-thumb {
  background: rgba(148,163,184,0.28);
  border-radius: 99px;
}

.sf-item {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 7px;
  padding: 8px 9px;
  border-radius: 8px;
  border: 1px solid transparent;
  background: transparent;
  cursor: pointer;
  font-family: inherit;
  text-align: left;
  transition: background 0.13s ease, border-color 0.13s ease, box-shadow 0.13s ease;
  margin-bottom: 2px;
  position: relative;
}

.sf-item:hover {
  background: rgba(37,99,235,0.05);
  border-color: rgba(37,99,235,0.10);
}

.sf-item.sf-selected {
  background: linear-gradient(135deg, rgba(37,99,235,0.11), rgba(79,70,229,0.07));
  border-color: rgba(37,99,235,0.22);
  box-shadow: 0 2px 10px rgba(37,99,235,0.10);
}

.sf-badge {
  flex-shrink: 0;
  font-size: 8px;
  font-weight: 900;
  padding: 2px 5px;
  border-radius: 99px;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.sf-badge-l0 { background: rgba(79,70,229,0.10); color: #4338ca; }
.sf-badge-l1 { background: rgba(37,99,235,0.10); color: #1d4ed8; }
.sf-badge-l2 { background: rgba(6,182,212,0.10);  color: #0891b2; }
.sf-badge-l3 { background: rgba(16,185,129,0.10); color: #047857; }
.sf-badge-lx { background: rgba(148,163,184,0.12); color: #64748b; }

.sf-selected .sf-badge {
  background: rgba(37,99,235,0.16) !important;
  color: #1d4ed8 !important;
}

.sf-item-body {
  flex: 1;
  min-width: 0;
}

.sf-item-name {
  display: block;
  font-size: 12px;
  font-weight: 800;
  color: #0f172a;
  line-height: 1.3;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.sf-selected .sf-item-name {
  color: #1d4ed8;
}

.sf-item-meta {
  display: block;
  font-size: 10px;
  color: #94a3b8;
  margin-top: 1px;
  font-weight: 700;
  white-space: nowrap;
}

.sf-arrow {
  flex-shrink: 0;
  font-size: 9px;
  color: #cbd5e1;
  transition: color 0.14s, transform 0.14s;
  margin-left: auto;
}

.sf-selected .sf-arrow {
  color: #2563eb;
  transform: translateX(2px);
}

.sf-arrow-hidden {
  opacity: 0;
  pointer-events: none;
}

.sf-divider {
  flex-shrink: 0;
  width: 1px;
  background: rgba(148,163,184,0.14);
  align-self: stretch;
}

.subject-flow-column:first-child .subject-flow-column-title {
  align-self: flex-start;
}

 .subject-flow-column:first-child .subject-flow-card {
  margin-top: auto;
  margin-bottom: auto;
  }

      .kpi-attributes-modal .modal-header {
        padding: 16px 20px 12px;
      }

      .kpi-attributes-modal .modal-header h2 {
        font-size: 24px;
      }

      .kpi-attributes-modal .modal-subtitle {
        font-size: 13px;
      }

.kpi-attributes-modal .modal-body {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 6px;
  overflow-y: auto;
  overflow-x: hidden;
  min-height: 0;
}

       .kpi-attributes-modal .form-section:first-of-type {
  min-height: 0;
  display: flex;
  flex-direction: column;
}

.kpi-attributes-modal .hierarchy-tree-field,
.kpi-attributes-modal .tree-select,
.kpi-attributes-modal .tree-select-panel {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
}

.kpi-attributes-modal .tree-list {
  flex: 1;
  height: auto !important;
  max-height: none !important;
  min-height: 360px;
  overflow: auto;
}

      .kpi-attributes-modal .modal-overview {
        gap: 6px;
      }

      .kpi-attributes-modal .overview-card {
        padding: 7px 10px;
      }

      .kpi-attributes-modal .form-section {
        margin: 0 !important;
        padding: 6px 10px !important;
      }

      .kpi-attributes-modal .form-grid {
        gap: 6px;
      }

      .kpi-attributes-modal .field {
        gap: 3px;
      }

      .kpi-attributes-modal .field label {
        font-size: 9px;
      }

      .kpi-attributes-modal .field .hint {
        font-size: 8px;
      }

      .kpi-attributes-modal .field input,
      .kpi-attributes-modal .field textarea,
      .kpi-attributes-modal .field select {
        padding: 6px 8px;
        font-size: 12px;
        border-radius: 9px;
      }

      .kpi-attributes-modal .field textarea {
        min-height: 42px;
      }

      .kpi-attributes-modal .hierarchy-tree-field .tree-select-trigger {
        min-height: 62px;
        padding: 10px 14px;
      }

      .kpi-attributes-modal .tree-select-toolbar {
        padding: 8px 10px;
      }

      .kpi-attributes-modal .tree-select-current-path {
        padding: 8px 10px;
        font-size: 11px;
      }

     .kpi-attributes-modal .tree-list {
        height: auto;
        max-height: 260px;
       }
      .kpi-attributes-modal .subject-flow {
        gap: 16px;
      }

      .kpi-attributes-modal .subject-flow-column {
        width: 220px;
        flex: 0 0 220px;
      }

      .kpi-attributes-modal .subject-flow-column-title {
        margin-bottom: 8px;
        padding: 6px 10px;
        font-size: 9px;
      }

      .kpi-attributes-modal .subject-flow-card {
        margin-bottom: 8px;
        padding: 10px 11px;
        border-radius: 14px;
      }

      .kpi-attributes-modal .subject-flow-kicker {
        margin-bottom: 4px;
        font-size: 8px;
      }

      .kpi-attributes-modal .subject-flow-name {
        font-size: 12px;
      }

      .kpi-attributes-modal .subject-flow-path {
        margin-top: 3px;
        font-size: 10px;
      }

.kpi-attributes-modal .modal-footer {
  display: flex;
  justify-content: flex-start;
  align-items: center;
  gap: 10px;
  width: 100%;
  position: relative;
  bottom: auto;
  padding: 14px 20px;
  border-top: 1px solid rgba(15,23,42,0.06);
  background: rgba(255,255,255,0.97);
}


.kpi-attributes-modal .footer-actions {
  display: flex;
  gap: 10px;
}

.kpi-attributes-modal #deleteBtn {
  display: none;
}

.parameter-modal #parameterDeleteBtn {
  display: none;
}
/* ===============================
   PREMIUM ADD / EDIT KPI MODAL
   Keeps tree always visible
================================ */

.modal-backdrop {
  background: rgba(15, 23, 42, 0.52) !important;
  backdrop-filter: blur(14px) saturate(140%) !important;
}

.modal-backdrop.open .kpi-attributes-modal {
  animation: modalSlideUp 0.35s cubic-bezier(0.22, 1, 0.36, 1);
}

.kpi-attributes-modal {
  width: min(1860px, calc(100vw - 48px)) !important;
  max-height: calc(100vh - 32px) !important;
  height: auto !important;
  border-radius: 32px !important;
  overflow: hidden !important;
  background:
    radial-gradient(circle at top right, rgba(37, 99, 235, 0.12), transparent 34%),
    linear-gradient(180deg, #ffffff 0%, #f8fbff 100%) !important;
  border: 1px solid rgba(255,255,255,0.9) !important;
  box-shadow: 0 40px 100px rgba(15, 23, 42, 0.32) !important;
}

.kpi-attributes-modal .modal-header {
  padding: 24px 30px 18px !important;
  background: rgba(255,255,255,0.96) !important;
  border-bottom: 1px solid rgba(148,163,184,0.18) !important;
}

.kpi-attributes-modal .modal-header h2 {
  font-size: 32px !important;
  font-weight: 950 !important;
  letter-spacing: -0.05em !important;
  color: #0f172a !important;
}

.kpi-attributes-modal .modal-subtitle {
  color: #64748b !important;
  font-size: 14px !important;
}

.kpi-attributes-modal .modal-body {
  display: block !important;
  max-height: calc(100vh - 190px) !important;
  overflow-y: auto !important;
  padding: 12px 20px 14px !important;
  background: linear-gradient(180deg, #fbfdff 0%, #f4f8fd 100%) !important;
}

.kpi-attributes-modal .form-section {
  margin: 0 0 10px 0 !important;
  padding: 12px !important;
  border-radius: 22px !important;
  background: rgba(255,255,255,0.9) !important;
  border: 1px solid rgba(148,163,184,0.20) !important;
  box-shadow: 0 14px 32px rgba(15,23,42,0.06) !important;
}

/* Keep tree clear and visible */
.kpi-attributes-modal .form-section:first-of-type {
  padding: 14px !important;
}

.kpi-attributes-modal .tree-select-panel {
  display: block !important;
  border-radius: 22px !important;
  overflow: hidden !important;
  background: #ffffff !important;
  border: 1px solid rgba(148,163,184,0.20) !important;
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.9) !important;
}

.kpi-attributes-modal .tree-list {
  height: 345px !important;
  min-height: 345px !important;
  max-height: 345px !important;
  overflow: auto !important;
  padding: 10px !important;
  background:
    radial-gradient(circle at top right, rgba(37,99,235,0.05), transparent 28%),
    linear-gradient(180deg, #ffffff 0%, #f8fbff 100%) !important;
}

/* Tree column layout */
.kpi-attributes-modal .sf-flow {
  display: flex !important;
  align-items: stretch !important;
  gap: 0 !important;
  min-width: max-content !important;
}

.kpi-attributes-modal .sf-column {
  width: 245px !important;
  flex: 0 0 245px !important;
  background: rgba(255,255,255,0.96) !important;
  border-right: 1px solid rgba(148,163,184,0.16) !important;
  box-shadow: 8px 0 20px rgba(15,23,42,0.025) !important;
}

.kpi-attributes-modal .sf-col-head {
  padding: 14px 16px !important;
  background: linear-gradient(180deg, #f8fbff 0%, #eef5ff 100%) !important;
  border-bottom: 1px solid rgba(148,163,184,0.16) !important;
}

.kpi-attributes-modal .sf-col-level {
  font-size: 10px !important;
  font-weight: 950 !important;
  color: #94a3b8 !important;
  letter-spacing: 0.12em !important;
}

.kpi-attributes-modal .sf-col-title {
  margin-top: 3px !important;
  font-size: 12px !important;
  font-weight: 950 !important;
  color: #1d4ed8 !important;
}

.kpi-attributes-modal .sf-col-count {
  margin-top: 4px !important;
  font-size: 11px !important;
  color: #64748b !important;
}

/* Tree items */
.kpi-attributes-modal .sf-item {
  margin-bottom: 6px !important;
  padding: 11px 12px !important;
  border-radius: 14px !important;
  border: 1px solid transparent !important;
  transition: all 0.2s ease !important;
}

.kpi-attributes-modal .sf-item:hover {
  background: rgba(37,99,235,0.07) !important;
  border-color: rgba(37,99,235,0.16) !important;
  transform: translateX(4px) !important;
}

.kpi-attributes-modal .sf-selected {
  background: linear-gradient(135deg, rgba(37,99,235,0.16), rgba(6,182,212,0.10)) !important;
  border-color: rgba(37,99,235,0.35) !important;
  box-shadow: 0 10px 24px rgba(37,99,235,0.16) !important;
}

.kpi-attributes-modal .sf-badge {
  border-radius: 999px !important;
  padding: 3px 7px !important;
  font-size: 9px !important;
}

.kpi-attributes-modal .sf-item-name {
  font-size: 13px !important;
  font-weight: 900 !important;
  color: #0f172a !important;
}

.kpi-attributes-modal .sf-item-meta {
  margin-top: 2px !important;
  font-size: 11px !important;
  color: #94a3b8 !important;
}

.kpi-attributes-modal .sf-arrow {
  color: #94a3b8 !important;
  font-size: 11px !important;
}

/* Inputs */
.kpi-attributes-modal .field input,
.kpi-attributes-modal .field select,
.kpi-attributes-modal .field textarea {
  min-height: 40px !important;
  border-radius: 13px !important;
  border: 1px solid rgba(148,163,184,0.28) !important;
  background: #ffffff !important;
  padding: 9px 11px !important;
  font-size: 13px !important;
  transition: all 0.2s ease !important;
}

.kpi-attributes-modal .field input:focus,
.kpi-attributes-modal .field select:focus,
.kpi-attributes-modal .field textarea:focus {
  border-color: rgba(37,99,235,0.55) !important;
  box-shadow: 0 0 0 4px rgba(37,99,235,0.12) !important;
  transform: translateY(-1px) !important;
}

/* Footer */
.kpi-attributes-modal .modal-footer {
  padding: 18px 24px !important;
  background: rgba(255,255,255,0.96) !important;
  border-top: 1px solid rgba(148,163,184,0.18) !important;
}

.kpi-attributes-modal .btn {
  border-radius: 16px !important;
  padding: 13px 20px !important;
  font-weight: 900 !important;
}

.kpi-attributes-modal .btn-primary {
  background: linear-gradient(135deg, #4f46e5, #2563eb, #06b6d4) !important;
  box-shadow: 0 16px 32px rgba(37,99,235,0.28) !important;
}

@keyframes modalSlideUp {
  from {
    opacity: 0;
    transform: translateY(26px) scale(0.97);
  }
  to {
    opacity: 1;
    transform: translateY(0) scale(1);
  }
}

/* Show full modal content without internal scrolling */
.kpi-attributes-modal {
  width: min(1860px, calc(100vw - 32px)) !important;
  height: auto !important;
  max-height: none !important;
  overflow: visible !important;
}

.kpi-attributes-modal .modal-body {
  max-height: none !important;
  height: auto !important;
  overflow: visible !important;
  padding: 10px 18px 12px !important;
}

/* Remove tree scroll */
.kpi-attributes-modal .tree-list {
  height: auto !important;
  min-height: auto !important;
  max-height: none !important;
  overflow: visible !important;
}

/* Show all tree columns/items clearly */
.kpi-attributes-modal .sf-flow {
  height: auto !important;
  min-height: auto !important;
  overflow: visible !important;
}

.kpi-attributes-modal .sf-column {
  height: auto !important;
  max-height: none !important;
  overflow: visible !important;
}

.kpi-attributes-modal .sf-col-body {
  height: auto !important;
  max-height: none !important;
  overflow: visible !important;
}

/* Make sections compact so everything fits */
.kpi-attributes-modal .form-section {
  margin-bottom: 8px !important;
  padding: 10px 12px !important;
}

.kpi-attributes-modal .tree-select-panel {
  overflow: visible !important;
}

.kpi-attributes-modal .field input,
.kpi-attributes-modal .field select,
.kpi-attributes-modal .field textarea {
  min-height: 36px !important;
  padding: 7px 10px !important;
}

.kpi-attributes-modal .modal-footer {
  padding: 12px 20px !important;
}


/* ===============================
   MODERN KPI TARGET ALLOCATION MODAL
================================ */

#parameterModalBackdrop {
  background: rgba(15, 23, 42, 0.55) !important;
  backdrop-filter: blur(14px) saturate(140%) !important;
}

#parameterModalBackdrop.open .parameter-modal {
  animation: parameterModalIn 0.35s cubic-bezier(0.22, 1, 0.36, 1);
}

.parameter-modal {
  width: min(1500px, calc(100vw - 42px)) !important;
  max-height: calc(100vh - 34px) !important;
  overflow: hidden !important;
  border-radius: 34px !important;
  background:
    radial-gradient(circle at top right, rgba(37,99,235,0.14), transparent 34%),
    radial-gradient(circle at bottom left, rgba(6,182,212,0.12), transparent 30%),
    linear-gradient(180deg, #ffffff 0%, #f8fbff 100%) !important;
  border: 1px solid rgba(255,255,255,0.88) !important;
  box-shadow: 0 40px 100px rgba(15,23,42,0.30) !important;
}

.parameter-modal .modal-header {
  padding: 26px 32px 20px !important;
  background: rgba(255,255,255,0.96) !important;
  border-bottom: 1px solid rgba(148,163,184,0.18) !important;
}

.parameter-modal .modal-header h2 {
  font-size: 30px !important;
  font-weight: 950 !important;
  letter-spacing: -0.05em !important;
  color: #0f172a !important;
}

.parameter-modal .modal-subtitle {
  margin-top: 6px !important;
  color: #64748b !important;
  font-size: 14px !important;
}

.parameter-modal .modal-body {
  padding: 18px 22px !important;
  max-height: calc(100vh - 210px) !important;
  overflow-y: auto !important;
  background: linear-gradient(180deg, #fbfdff 0%, #f4f8fd 100%) !important;
}

.parameter-modal .form-section {
  margin-bottom: 16px !important;
  padding: 18px !important;
  border-radius: 24px !important;
  background: rgba(255,255,255,0.9) !important;
  border: 1px solid rgba(148,163,184,0.20) !important;
  box-shadow: 0 16px 34px rgba(15,23,42,0.06) !important;
}

.parameter-modal .form-grid {
  gap: 14px !important;
}

.parameter-modal .field label {
  font-size: 10px !important;
  font-weight: 900 !important;
  color: #334155 !important;
}

.parameter-modal .field .hint {
  font-size: 9px !important;
  color: #94a3b8 !important;
}

.parameter-modal .field input,
.parameter-modal .field select,
.parameter-modal .field textarea {
  min-height: 42px !important;
  border-radius: 14px !important;
  border: 1px solid rgba(148,163,184,0.28) !important;
  background: #ffffff !important;
  padding: 10px 12px !important;
  font-size: 13px !important;
  color: #0f172a !important;
  transition: all 0.2s ease !important;
}

.parameter-modal .field textarea {
  min-height: 80px !important;
  resize: vertical !important;
}

.parameter-modal .field input:focus,
.parameter-modal .field select:focus,
.parameter-modal .field textarea:focus {
  border-color: rgba(37,99,235,0.55) !important;
  box-shadow: 0 0 0 4px rgba(37,99,235,0.12) !important;
  transform: translateY(-1px) !important;
}

.parameter-modal .modal-footer {
  padding: 18px 24px !important;
  background: rgba(255,255,255,0.96) !important;
  border-top: 1px solid rgba(148,163,184,0.18) !important;
  display: flex !important;
  justify-content: space-between !important;
  align-items: center !important;
}

.parameter-modal .btn {
  border-radius: 16px !important;
  padding: 13px 20px !important;
  font-weight: 900 !important;
}

.parameter-modal .btn-primary {
  background: linear-gradient(135deg, #4f46e5, #2563eb, #06b6d4) !important;
  box-shadow: 0 16px 32px rgba(37,99,235,0.28) !important;
}

.parameter-modal .btn-danger {
  background: linear-gradient(135deg, #fb7185, #ef4444) !important;
  color: white !important;
  box-shadow: 0 14px 28px rgba(239,68,68,0.22) !important;
}

@keyframes parameterModalIn {
  from {
    opacity: 0;
    transform: translateY(26px) scale(0.97);
  }
  to {
    opacity: 1;
    transform: translateY(0) scale(1);
  }
}
 </style>
  </head>

  <body>
    <div class="layout">
      <aside class="sidebar">
        <div class="brand">
          <div class="brand-badge">K</div>
          <div>
            <h2>KPI Suite</h2>
            <p>Performance workspace</p>
          </div>
        </div>

        <div>
          <div class="sidebar-section-title">Navigation</div>
          <nav class="nav">
        

            <a href="#" class="nav-item" onclick="openCreateModal(); return false;">
              <div class="nav-icon">➕</div>
              <span>Create KPI</span>
            </a>
              <a href="#" class="nav-item active" id="navDashboard" onclick="showDashboard(); return false;">
              <div class="nav-icon">📊</div>
              <span>Consult KPI</span>
            </a>

            <a href="#" class="nav-item" onclick="openCreateParameterModal(); return false;">
              <div class="nav-icon">🧭</div>
              <span>Create KPI TARGET ALLOCATION</span>
            </a>

            <a href="#" class="nav-item" id="navConsultAllocations" onclick="showConsultTargetAllocations(); return false;">
              <div class="nav-icon">📁</div>
              <span>Consult Target Allocation</span>
            </a>
          </nav>
        </div>

        <div class="sidebar-footer">
          <h4>Responsible Workspace</h4>
          <p>
            Manage KPI definitions, update targets and keep your performance indicators organized in one modern dashboard.
          </p>
        </div>
      </aside>

      <main class="content">
        <div class="topbar">
          <div class="topbar-left">
              <h1>Responsible KPI Dashboard</h1>
            <p>Modern KPI workspace for ${escapeHtml(responsibleName)}. KPI creation and target allocation are locked to this responsible.</p>
          </div>

        </div>

        <section id="dashboardSection">
          <div class="hero-panel">
            <div class="hero-row">
              <div class="hero-meta">
                <h3>Executive KPI Management</h3>
            </div>
          </div>

          <div class="stats-grid">
            <div class="stat-card">
              <div class="stat-label">Total KPIs</div>
              <div class="stat-value" id="statTotal">0</div>
            </div>

            <div class="stat-card">
              <div class="stat-label">Monthly KPIs</div>
              <div class="stat-value" id="statMonthly">0</div>
            </div>

            <div class="stat-card">
              <div class="stat-label">With Target</div>
              <div class="stat-value" id="statTarget">0</div>
            </div>

            <div class="stat-card">
              <div class="stat-label">Tolerance Rules</div>
              <div class="stat-value" id="statTolerance">0</div>
            </div>
          </div>

          <div class="toolbar">
            <div class="search-wrap">
              <input id="search" class="search" placeholder="Search KPI by title, subject, frequency..." />
            </div>
          </div>

          <div class="section-head">
            <div>
              <h2>KPI Portfolio</h2>
              <p>One KPI per line with the title, subtitle and a quick edit action.</p>
            </div>
          </div>

          <div id="grid" class="grid"></div>
        </section>

        <section id="myKpisSection" style="display:none;">
          <div class="section-head">
            <div>
              <h2>My KPI Analytics</h2>
              <p>Visual trends of KPI values by month for this responsible.</p>
            </div>
          </div>

          <div id="chartsGrid" class="grid"></div>
        </section>

        <section id="consultTargetAllocationsSection" style="display:none;">
          <div class="section-head">
            <div>
              <h2>Consult Target Allocation</h2>
              <p>Consult existing target allocations and quickly edit or delete them.</p>
            </div>
          </div>

          <div class="toolbar">
            <div class="search-wrap">
              <input id="parameterSearch" class="search" placeholder="Search target allocation by KPI, subject, scope..." />
            </div>
          </div>

          <div class="parameter-section-shell">
            <div id="parameterGrid" class="grid"></div>
          </div>
        </section>
      </main>
    </div>

    <div id="modalBackdrop" class="modal-backdrop">
      <div class="modal kpi-attributes-modal">
        <div class="modal-header">
          <div class="modal-title-wrap">
            <h2 id="modalTitle">Edit KPI Attributes</h2>
            <div class="modal-subtitle" id="modalSubtitle">
              Update KPI information, targets, calculation rules and thresholds in a cleaner professional form.
            </div>
          </div>

          <button class="btn btn-soft" onclick="closeModal()">Close</button>
        </div>

      <div class="modal-body">
          <input type="hidden" id="kpi_id" />

       

          <div class="form-section">
            <div class="form-grid">
            <div class="field col-12 hierarchy-field hierarchy-tree-field">
              <label><span>Subject</span><span class="hint">Excel hierarchy tree</span></label>
              <input type="hidden" id="subject" />
              <input type="hidden" id="subject_node_id" />
              <div class="tree-select" id="subjectTreeSelect">
                <button type="button" class="tree-select-trigger" id="subjectTreeTrigger" onclick="toggleSubjectTree()">
                  <span class="tree-select-value">
                    <span class="tree-select-label" id="subjectTreeLabel">Select subject from tree</span>
                    <span class="tree-select-meta" id="subjectTreeMeta">Open the visual hierarchy to choose a subject and follow its branch.</span>
                  </span>
                  <span class="tree-select-chevron" id="subjectTreeChevron">▾</span>
                </button>
                <div class="tree-select-panel" id="subjectTreePanel">
           <div class="tree-select-toolbar">
           <div class="vtree-search-row">
            <input
            type="text"
            class="vtree-search-input"
            id="subjectTreeSearch"
            placeholder="Filter subjects..."
            oninput="onSubjectTreeSearch(this.value)"
            autocomplete="off"
            />
           <div class="vtree-toolbar-actions">
            <button type="button" class="v-tree-action-btn" onclick="resetSubjectFlow()">&#10006; Reset</button>
           </div>
          </div>
         <div class="tree-select-current-path" id="subjectTreeCurrentPath">Selected path: No subject selected yet.</div>
          </div>
                <div class="tree-select-fit" id="subjectTreeFitMeta">Flow view</div>
                  </div>
                  <div class="tree-list" id="subjectTreeList"></div>
                </div>
              </div>
            </div>

       
          </div>
          

           <div class="form-section">     
           <div class="form-grid kpi-inline-row">
             <div class="field col-4">
              <label><span>KPI Name</span><span class="hint">Required KPI label</span></label>
              <input id="indicator_sub_title" placeholder="Type or adjust the KPI name" />
              </div>

              <div class="field col-2">
              <label><span>KPI Code</span><span class="hint">Optional reference</span></label>
              <input id="kpi_code" placeholder="e.g. KPI-001" />
              </div>

              <div class="field col-2">
              <label><span>Unit</span><span class="hint">Measure</span></label>
               <select id="unit">
               <option value="%">%</option>
               <option value="Pieces">Pieces</option>
               <option value="PPM">PPM</option>
              <option value="Number">Number</option>
              </select>
             </div>

           <div class="field col-2">
            <label><span>Frequency</span><span class="hint">Rhythm</span></label>
            <select id="frequency">
            <option value="By Monthly">By Monthly</option>
            <option value="Monthly">Monthly</option>
            <option value="By Weekly">By Weekly</option>
            <option value="Weekly">Weekly</option>
            <option value="Yearly">Yearly</option>
            <option value="By Yearly">By Yearly</option>
            </select>
           </div>

           <div class="field col-2">
            <label><span>Direction</span><span class="hint">Trend direction</span></label>
           <select id="direction">
           <option value="Up">Up</option>
             <option value="Down">Down</option>
             </select>
            </div>

           <div class="field col-2">
            <label><span>Status</span><span class="hint">Lifecycle state</span></label>
            <select id="status">
             <option value="Active">Active</option>
             <option value="Draft">Draft</option>
             <option value="Inactive">Inactive</option>
             <option value="Archived">Archived</option>
             </select>
             </div>

              <div class="field col-5">
                <label><span>Definition</span><span class="hint">Explain clearly</span></label>
                <textarea id="definition" placeholder="Write a clear KPI definition..."></textarea>
              </div>
           
            </div>
         </div>

           <div class="form-section">
            <div class="form-grid">
       
         <div class="field col-3">
               <label><span>Calculation On</span><span class="hint">Basis</span></label>
               <select id="calculation_on" onchange="handleCalculationOnChange()">
               <option value="Value">Value</option>
               <option value="Average">Average</option>
               </select>
              </div>
              <div class="field col-2" id="wrap_nombre_periode">
             <label><span>Nombre periode</span><span class="hint">Period</span></label>
             <select id="nombre_periode">
             <option value="1 week">1 week</option>
             <option value="2 weeks">2 weeks</option>
             <option value="3 weeks">3 weeks</option>
             <option value="4 weeks">4 weeks</option>
             </select>
             </div>
              <div class="field col-2">
                <label><span>Calculation Mode</span><span class="hint">Mode</span></label>
                  <select id="calculation_mode" onchange="handleCalculationModeChange()">
                  <option value="Direct">Direct</option>
                  <option value="Ratio">Ratio</option>
                  </select>
               </div>

              <div class="field col-2" id="wrap_reference_kpi_id" style="display:none;">
                <label><span>Reference KPI</span><span class="hint">Required for ratio</span></label>
                <select id="reference_kpi_id" onchange="updateDisplayedKpiPreview()">
                  <option value="">Select reference KPI</option>
                </select>
              </div>
              <div class="field col-2">
              <label><span>Display Trend</span><span class="hint">Yes / No</span></label>
             <select id="display_trend" onchange="handleDisplayTrendChange()">
             <option value="No">No</option>
             <option value="Yes">Yes</option>
            </select>
             </div>

          <div class="field col-2" id="wrap_regression" style="display:none;">
           <label><span>Regression</span><span class="hint">Weeks</span></label>
            <select id="regression">
            <option value="1 week">1 week</option>
           <option value="2 weeks">2 weeks</option>
           <option value="3 weeks">3 weeks</option>
           <option value="4 weeks">4 weeks</option>
           </select>
           </div>

  
          <div class="field col-2">
          <label><span>Min</span><span class="hint">Optional</span></label>
          <select id="min_type" onchange="handleMinTypeChange()">
           <option value="Null">Null</option>
           <option value="0">0</option>
           <option value="Valeur a entrer">Valeur a entrer</option>
          </select>
          </div>
              <div class="field col-2" id="wrap_min_value" style="display:none;">
                <label><span>Min Value</span><span class="hint">Enter value</span></label>
                <input id="min" placeholder="Min value" />
              </div>
            
              <div class="field col-3">
                <label><span>Target Auto Adjust</span><span class="hint">Optional</span></label>
                <select id="target_auto_adjustment">
                  <option value="Yes">Yes</option>
                  <option value="No">No</option>
                </select>
              </div>
              <div class="field col-2">
                <label><span>Max</span><span class="hint">Optional</span></label>
                <select id="max_type" onchange="handleMaxTypeChange()">
                  <option value="Null">Null</option>
                  <option value="0">0</option>
                  <option value="Valeur a entrer">Valeur a entrer</option>
                </select>
              </div>
              <div class="field col-2" id="wrap_max_value" style="display:none;">
                <label><span>Max Value</span><span class="hint">Enter value</span></label>
                <input id="max" placeholder="Max value" />
              </div>

              <div class="field col-2">
                <label><span>Formula</span><span class="hint">Optional calculation rule</span></label>
                <textarea id="kpi_formula" placeholder="Describe the KPI formula or calculation logic..."></textarea>
              </div>
           
            </div>
          </div>

          <div class="form-section">
           
            <div class="form-grid">
              <div class="field col-3">
                <label><span>Tolerance Type</span><span class="hint">Relative / Absolute</span></label>
                <select id="tolerance_type" onchange="handleToleranceTypeChange()">
                  <option value="Relative">Relative</option>
                  <option value="Absolute">Absolute</option>
                </select>
              </div>

              <!-- Shown for Relative only -->
              <div class="field col-3" id="wrap_up_tolerance">
                <label><span>Up Tolerance</span><span class="hint">Upper variance</span></label>
                <input id="up_tolerance" placeholder="10% or 10" oninput="recalculateLimits(this)" onkeyup="recalculateLimits(this)" />
              </div>
              <div class="field col-3" id="wrap_low_tolerance">
                <label><span>Low Tolerance</span><span class="hint">Lower variance</span></label>
                <input id="low_tolerance" placeholder="-10% or -10" oninput="recalculateLimits(this)" onkeyup="recalculateLimits(this)" />
              </div>

              <!-- Shown for Absolute only -->
              <div class="field col-3" id="wrap_high_limit" style="display:none;">
                <label><span>High Limit</span><span class="hint">Enter value</span></label>
                <input id="high_limit" type="number" step="any" placeholder="e.g. 544.5" />
              </div>
              <div class="field col-3" id="wrap_low_limit" style="display:none;">
                <label><span>Low Limit</span><span class="hint">Enter value</span></label>
                <input id="low_limit" type="number" step="any" placeholder="e.g. 445.5" />
              </div>

              <div class="field col-3">
                <label><span>Owner Role</span><span class="hint">Optional accountability</span></label>
                <select id="owner_role_id">
                  <option value="">Select owner role</option>
                </select>
              </div>

              <div class="field col-6">
                <label><span>Comments</span><span class="hint">Optional extra notes</span></label>
                <textarea id="comments" placeholder="Add context, ownership notes or implementation comments..."></textarea>
              </div>
              </div>
            </div>
          </div>

       <div class="modal-footer">
      <div class="footer-actions">
      <button id="deleteBtn" class="btn btn-danger" onclick="deleteCurrentKpi()">Delete KPI</button>
      <button class="btn btn-soft" onclick="closeModal()">Cancel</button>
    <button class="btn btn-primary" onclick="saveKpi()">Save KPI</button>
  </div>
</div>
        </div>

    
      </div>
    </div>

    <div id="parameterModalBackdrop" class="modal-backdrop">
      <div class="modal parameter-modal">
        <div class="modal-header">
          <div class="modal-title-wrap">
            <h2 id="parameterModalTitle">Create KPI Target Allocation</h2>
            <div class="modal-subtitle" id="parameterModalSubtitle">
              Create a KPI target allocation with scope, target dates and approval details.
            </div>
          </div>

          <button class="btn btn-soft" onclick="closeParameterModal()">Close</button>
        </div>

        <div class="modal-body">
          <input type="hidden" id="parameter_object_id" />
          <div class="form-section">
            <div class="form-grid">
              <div class="field col-6">
                <label><span>KPI Name</span><span class="hint">Last subject above each KPI name</span></label>
                <select id="parameter_kpi_id"></select>
              </div>
              <div class="field col-3">
                <label><span>KPI Type</span><span class="hint">Optional classification</span></label>
                <select id="parameter_kpi_type">
                  <option value="">Select KPI Type</option>
                  <option value="Multisite">Multisite</option>
                  <option value="Individual">Individual</option>
                  <option value="Zone">Zone</option>
                </select>
              </div>
         
              <div class="field col-4" id="parameter_plant_field">
                <label><span>Unit</span></label>
                <select id="parameter_plant_id"></select>
              </div>
              <div class="field col-4" id="parameter_zone_field">
                <label><span>Zone</span><span class="hint">Optional zone scope</span></label>
                <select id="parameter_zone_id"></select>
              </div>
              <div class="field col-4" id="parameter_function_field">
                <label><span id="parameter_function_label">Function</span><span class="hint" id="parameter_function_hint">Optional business function</span></label>
                <input id="parameter_function" placeholder="Manufacturing, Finance, Quality..." />
              </div>
              <div class="field col-3">
                <label><span>Product Line</span><span class="hint">Optional scope</span></label>
                <select id="parameter_product_line_id"></select>
              </div>
              <div class="field col-3">
                <label><span>Product</span><span class="hint">Optional product scope</span></label>
                <select id="parameter_product_id"></select>
              </div>

              <div class="field col-3">
                <label><span>Market</span><span class="hint">Optional market scope</span></label>
                <select id="parameter_market_id"></select>
              </div>
          
            </div>

          </div>

          <div class="form-section">
            <div class="form-grid">

              <div class="field col-3">
                <label><span>Target Status</span><span class="hint">Required workflow status</span></label>
                <select id="parameter_target_status">
                  <option value="Draft">Draft</option>
                  <option value="Active">Active</option>
                  <option value="Approved">Approved</option>
                  <option value="Closed">Closed</option>
                </select>
              </div>
              <div class="field col-3">
                <label><span>Target Value</span><span class="hint">Required allocation target</span></label>
                <input id="parameter_target_value" type="number" step="any" placeholder="3000" />
              </div>
              <div class="field col-3">
                <label><span>Target Unit</span><span class="hint">Usually copied from KPI</span></label>
                <select id="parameter_target_unit">
                  <option value="">Select target unit</option>
                  <option value="%">%</option>
                  <option value="PPM">PPM</option>
                  <option value="Pieces">Pieces</option>
                  <option value="Number">Number</option>
                </select>
              </div>
              <div class="field col-3">
                <label><span>Local Currency</span><span class="hint">USD, EUR, MAD, NGN</span></label>
                <input id="parameter_local_currency" placeholder="USD" />
              </div>
              <div class="field col-3">
                <label><span>Last Best Target</span><span class="hint">Optional benchmark</span></label>
                <input id="parameter_last_best_target" type="number" step="any" placeholder="3400" />
              </div>

              <div class="field col-3">
                <label><span>Previous Target</span><span class="hint">Earlier allocation value</span></label>
                <input id="parameter_previous_target_value" type="number" step="any" placeholder="2800" />
              </div>
              <div class="field col-3">
                <label><span>Setup Date</span><span class="hint">Required setup date</span></label>
                <input id="parameter_target_setup_date" type="date" />
              </div>
              <div class="field col-3">
                <label><span>Target Start Date</span><span class="hint">Optional effective date</span></label>
                <input id="parameter_target_start_date" type="date" />
              </div>
              <div class="field col-3">
                <label><span>Target End Date</span><span class="hint">Optional end date</span></label>
                <input id="parameter_target_end_date" type="date" />
              </div>

              <div class="field col-3">
                <label><span id="parameter_set_by_people_label">Responsible</span><span class="hint" id="parameter_set_by_people_hint">Assigned from current dashboard</span></label>
                <select id="parameter_set_by_people_id"></select>
              </div>
              <div class="field col-3">
                <label><span>Approved By</span><span class="hint">Optional approver</span></label>
                <select id="parameter_approved_by_people_id"></select>
              </div>

              <div class="field col-3">
                <label><span>Comments</span><span class="hint">Optional allocation notes</span></label>
                <textarea id="parameter_comments" placeholder="Add approval notes, scope details or rollout comments..."></textarea>
              </div>
            </div>
          </div>
        </div>

        <div class="modal-footer">
      
          <div class="footer-actions">
            <button class="btn btn-soft" onclick="closeParameterModal()">Cancel</button>
            <button class="btn btn-primary" onclick="saveParameterKpi()">Save Target Allocation</button>
          </div>
        </div>
      </div>
    </div>

    <div id="toast" class="toast"></div>

    <script>
      const responsibleId = ${JSON.stringify(String(dashboardResponsibleId || ""))};
      const responsibleName = ${JSON.stringify(responsibleName)};
      const kpiSubjectHierarchy = ${JSON.stringify(kpiSubjectHierarchy)};
      const roleOptions = ${JSON.stringify(roleOptions)};
      const allocationLookups = ${JSON.stringify(allocationLookups)};
      let currentRows = [];
      let referenceKpiRows = [];
      let currentParameterRows = [];
      let parameterSourceRows = [];
      let parameterKpiCatalog = [];
      let chartsLoaded = false;
      let chartInstances = [];

      function showToast(message) {
        const toast = document.getElementById("toast");
        toast.textContent = message;
        toast.style.display = "block";
        setTimeout(() => {
          toast.style.display = "none";
        }, 2400);
      }

      function buildParameterKpiOptionLabel(kpi) {
        return String(kpi.indicator_sub_title || "").trim() || "Untitled KPI";
      }

      function getParameterKpiGroupLabel(subjectPath) {
        const pathText = String(subjectPath || "").trim();
        if (!pathText) return "Other KPIs";

        const parts = pathText
          .split("/")
          .map((part) => String(part || "").trim())
          .filter(Boolean);

        return parts.length ? parts[parts.length - 1] : pathText;
      }

      function ensureParameterTargetUnitOption(unitValue) {
        const targetUnitSelect = document.getElementById("parameter_target_unit");
        const normalizedUnitValue = String(unitValue || "").trim();
        if (!targetUnitSelect || !normalizedUnitValue) return;

        const alreadyExists = Array.from(targetUnitSelect.options).some(
          (option) => String(option.value || "").trim() === normalizedUnitValue
        );

        if (!alreadyExists) {
          targetUnitSelect.insertAdjacentHTML(
            "beforeend",
            '<option value="' + escapeHtml(normalizedUnitValue) + '">' +
            escapeHtml(normalizedUnitValue) +
            '</option>'
          );
        }
      }

      function getParameterKpiById(kpiId) {
        return (parameterKpiCatalog || []).find(kpi => String(kpi.kpi_id) === String(kpiId)) || null;
      }

      function updateParameterKpiSummary() {
        const selectedKpi = getParameterKpiById(getParameterFieldValue("parameter_kpi_id"));
        const targetUnitInput = document.getElementById("parameter_target_unit");
        if (selectedKpi && targetUnitInput && !String(targetUnitInput.value || "").trim()) {
          ensureParameterTargetUnitOption(selectedKpi.unit || "");
          targetUnitInput.value = selectedKpi.unit || "";
        }
      }

      async function loadKpiNames(selectedValue = "") {
        const select = document.getElementById("parameter_kpi_id");
        if (!select) return;

        const res = await fetch("/api/kpis");
        const data = await res.json();
        parameterKpiCatalog = Array.isArray(data) ? data : [];

        const grouped = parameterKpiCatalog.reduce((acc, kpi) => {
          const subjectPath = String(kpi.subject || "Other KPIs").trim() || "Other KPIs";
          if (!acc[subjectPath]) acc[subjectPath] = [];
          acc[subjectPath].push(kpi);
          return acc;
        }, {});

        const groupMarkup = Object.keys(grouped)
          .sort((a, b) => a.localeCompare(b))
          .map(subjectPath => {
            const options = grouped[subjectPath]
              .sort((a, b) => {
                const nameCompare = buildParameterKpiOptionLabel(a).localeCompare(buildParameterKpiOptionLabel(b));
                if (nameCompare !== 0) return nameCompare;
                return String(a.indicator_title || "").localeCompare(String(b.indicator_title || ""));
              })
              .map(kpi =>
                '<option value="' + escapeHtml(kpi.kpi_id) + '">' +
                escapeHtml(buildParameterKpiOptionLabel(kpi)) +
                '</option>'
              ).join("");

            return '<optgroup label="' + escapeHtml(getParameterKpiGroupLabel(subjectPath)) + '">' + options + '</optgroup>';
          }).join("");

        select.innerHTML =
          '<option value="">Select KPI Name</option>' +
          groupMarkup;

        select.value = selectedValue || "";
        updateParameterKpiSummary();
      }

      function escapeHtml(value) {
        return String(value ?? "")
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/"/g, "&quot;")
          .replace(/'/g, "&#39;");
      }

  function splitMultiValue(value) {
  return String(value || "")
    .split(",")
    .map(v => v.trim())
    .filter(Boolean);
}

function getSelectedValues(selectId) {
  const el = document.getElementById(selectId);
  if (!el) return [];
  return Array.from(el.selectedOptions).map(o => o.value).filter(Boolean);
}

function setSelectOptions(selectId, options, placeholder, selectedValue = "") {
  const selectEl = document.getElementById(selectId);
  if (!selectEl) return;

  const uniqueOptions = Array.from(new Set((options || []).filter(Boolean)));

  selectEl.innerHTML =
    '<option value="">' + escapeHtml(placeholder) + '</option>' +
    uniqueOptions.map(option =>
      '<option value="' + escapeHtml(option) + '">' + escapeHtml(option) + '</option>'
    ).join("");

  selectEl.value = selectedValue || "";
  selectEl.disabled = uniqueOptions.length === 0;
}




function cleanLookupLabel(option, { stripTrailingNumericSuffix = false } = {}) {
  let label =
    option.unit_name ||
    option.plant_name ||
    option.zone_name ||
    option.product_line_name ||
    option.product_name ||
    option.market_name ||
    option.customer_name ||
    option.display_name ||
    option.name ||
    option.label ||
    option.value ||
    "";

  label = String(label).trim();

  if (stripTrailingNumericSuffix) {
    label = label.replace(/\s*[-:·•]\s*\d+\s*$/g, "").trim();
  }

  return label;
}

function dedupeLookupOptionsByLabel(options, selectedValue = "", cleanLabelOptions = {}) {
  const selectedText = selectedValue === null || selectedValue === undefined
    ? ""
    : String(selectedValue);
  const seenByLabel = new Map();

  (Array.isArray(options) ? options : []).forEach((option) => {
    if (!option || option.value === undefined || option.value === null) return;

    const label = cleanLookupLabel(option, cleanLabelOptions);
    const valueText = String(option.value);
    const existingOption = seenByLabel.get(label);

    if (!existingOption) {
      seenByLabel.set(label, option);
      return;
    }

    const existingValueText = String(existingOption.value);
    if (valueText === selectedText && existingValueText !== selectedText) {
      seenByLabel.set(label, option);
    }
  });

  return Array.from(seenByLabel.values());
}

function setLookupSelectOptions(selectId, options, placeholder, selectedValue = "") {
  const selectEl = document.getElementById(selectId);
  if (!selectEl) return;

  const baseOptions = Array.isArray(options)
    ? options.filter(option => option && option.value !== undefined && option.value !== null)
    : [];

  const selectedText = selectedValue === null || selectedValue === undefined ? "" : String(selectedValue);
  const shouldStripTrailingNumericSuffix =
    selectId === "parameter_plant_id" || selectId === "parameter_unit_id";
  const normalizedOptions = selectId === "parameter_zone_id"
    ? dedupeLookupOptionsByLabel(baseOptions, selectedText, {
      stripTrailingNumericSuffix: false
    })
    : baseOptions;

  selectEl.innerHTML =
    '<option value="">' + escapeHtml(placeholder) + '</option>' +
    normalizedOptions.map(option => {
      const label = cleanLookupLabel(option, {
        stripTrailingNumericSuffix: shouldStripTrailingNumericSuffix
      });

      return '<option value="' + escapeHtml(option.value) + '">' +
        escapeHtml(label) +
        '</option>';
    }).join("");

  if (selectedText && !normalizedOptions.some(option => String(option.value) === selectedText)) {
    selectEl.insertAdjacentHTML(
      "beforeend",
      '<option value="' + escapeHtml(selectedText) + '">' +
        escapeHtml(
          shouldStripTrailingNumericSuffix
            ? cleanLookupLabel({ label: selectedText }, { stripTrailingNumericSuffix: true })
            : selectedText
        ) +
      '</option>'
    );
  }

  selectEl.value = selectedText;
  selectEl.disabled = normalizedOptions.length === 0;
}

function populateRoleOptions(selectedValue = "") {
  setLookupSelectOptions("owner_role_id", roleOptions, "Select owner role", selectedValue);
}

function toggleScopedParameterField(fieldId, shouldShow) {
  const field = document.getElementById(fieldId);
  if (!field) return;

  field.classList.toggle("is-hidden", !shouldShow);

  const input = field.querySelector("input, select, textarea");
  if (!input) return;

  input.disabled = !shouldShow;
  if (!shouldShow) {
    input.value = "";
  }
}

function handleParameterKpiTypeChange() {
  const kpiType = getParameterFieldValue("parameter_kpi_type").toLowerCase();
  const isIndividual = kpiType === "individual";

  toggleScopedParameterField("parameter_plant_field", kpiType === "multisite");
  toggleScopedParameterField("parameter_zone_field", kpiType === "zone");
  toggleScopedParameterField("parameter_function_field", isIndividual);

  const functionField = document.getElementById("parameter_function_field");
  const functionLabel = document.getElementById("parameter_function_label");
  const functionHint = document.getElementById("parameter_function_hint");
  const functionInput = document.getElementById("parameter_function");

  if (functionField) {
    functionField.classList.toggle("col-8", isIndividual);
    functionField.classList.toggle("col-4", !isIndividual);
  }

  if (functionLabel) {
    functionLabel.textContent = isIndividual
      ? "Function / Department Concerned"
      : "Function";
  }

  if (functionHint) {
    functionHint.textContent = isIndividual
      ? "Use this when no specific unit is used"
      : "Optional business function";
  }

  if (functionInput) {
    functionInput.placeholder = isIndividual
      ? "Department concerned if no specific unit is used"
      : "Manufacturing, Finance, Quality...";
  }
}

const multisiteUnitResponsibleMap = {
  "Site-Anhui": "Allan RIEGEL",
  "Site-France-Poitiers": "Florence PARADIS",
  "Site-Germany": "Dagmar ANSINN",
  "Site-India": "Sridhar BOOVARAGHAVAN",
  "Site-Korea": "Samtak JOO",
  "Site-Kunshan": "Allan RIEGEL",
  "Site-Mexico": "Hector OLIVARES",
  "Site-Tianjin": "Yang YANG",
  "Site-Tunisia-SAME": "Imed Ben ALAYA",
  "Site-Tunisia-SameService": "Fethi CHAOUACHI",
  "Site-Tunisia-SCEET": "Imed Ben ALAYA"
};

function autoFillSetByFromUnit() {
  const normalizedKpiType = getParameterFieldValue("parameter_kpi_type").toLowerCase();
  const plantSelect = document.getElementById("parameter_plant_id");
  const setBySelect = document.getElementById("parameter_set_by_people_id");
  const setByHint = document.getElementById("parameter_set_by_people_hint");
  if (!plantSelect || !setBySelect || normalizedKpiType !== "multisite") return;

  const selectedUnitText =
    plantSelect.options[plantSelect.selectedIndex]?.textContent?.trim() || "";
  const personName = multisiteUnitResponsibleMap[selectedUnitText];
  if (!personName) return;

  const normalizeLabel = (value) =>
    String(value || "").trim().replace(/\s+/g, " ").toLowerCase();
  const matchingOption = Array.from(setBySelect.options).find(
    (option) => normalizeLabel(option.textContent) === normalizeLabel(personName)
  );

  if (matchingOption) {
    setBySelect.value = matchingOption.value;
    setBySelect.dataset.autoAssignedFromUnit = "true";
    if (setByHint) {
      setByHint.textContent = "Auto-filled from selected unit: " + personName + ". You can still change it.";
    }
    if (typeof updateParameterOverview === "function") {
      updateParameterOverview();
    }
  }
}

function syncDashboardResponsibleAssignment() {
  const setBySelect = document.getElementById("parameter_set_by_people_id");
  const setByHint = document.getElementById("parameter_set_by_people_hint");
  if (!setBySelect) return;

  const normalizedResponsibleId = String(responsibleId || "").trim();
  const hasDashboardResponsible = normalizedResponsibleId !== "";

  setBySelect.dataset.lockedResponsible = "false";

  if (setByHint) {
    setByHint.textContent = hasDashboardResponsible
      ? "Defaulted to " + (responsibleName || "current responsible") + ". You can change it."
      : "Select the person who will own this KPI";
  }

  if (!hasDashboardResponsible) {
    setBySelect.disabled = false;
    return;
  }

  const optionExists = Array.from(setBySelect.options).some(
    (option) => String(option.value || "") === normalizedResponsibleId
  );

  if (!optionExists) {
    setBySelect.insertAdjacentHTML(
      "beforeend",
      '<option value="' + escapeHtml(normalizedResponsibleId) + '">' +
      escapeHtml(responsibleName || ("Person " + normalizedResponsibleId)) +
      '</option>'
    );
  }

  if (!String(setBySelect.value || "").trim()) {
    setBySelect.value = normalizedResponsibleId;
  }
  setBySelect.disabled = false;
}

function populateAllocationLookupOptions(values = {}) {
  setLookupSelectOptions("parameter_plant_id", allocationLookups.plants, "Select unit", values.parameter_plant_id || values.plant_id || "");
  setLookupSelectOptions("parameter_zone_id", allocationLookups.zones, "Select zone", values.parameter_zone_id || values.zone_id || "");
  setLookupSelectOptions("parameter_unit_id", allocationLookups.units, "Select unit", values.parameter_unit_id || values.unit_id || "");
  setLookupSelectOptions("parameter_product_line_id", allocationLookups.productLines, "Select product line", values.parameter_product_line_id || values.product_line_id || "");
  setLookupSelectOptions("parameter_product_id", allocationLookups.products, "Select product", values.parameter_product_id || values.product_id || "");
  setLookupSelectOptions("parameter_market_id", allocationLookups.markets, "Select market", values.parameter_market_id || values.market_id || "");
  setLookupSelectOptions("parameter_customer_id", allocationLookups.customers, "Select customer", values.parameter_customer_id || values.customer_id || "");
  setLookupSelectOptions("parameter_set_by_people_id", allocationLookups.people, "Select person", values.parameter_set_by_people_id || values.set_by_people_id || "");
  setLookupSelectOptions("parameter_approved_by_people_id", allocationLookups.people, "Select approver", values.parameter_approved_by_people_id || values.approved_by_people_id || "");
  ensureParameterTargetUnitOption(values.parameter_target_unit || values.target_unit || "");
  syncDashboardResponsibleAssignment();
}
function getFieldValue(id) {
  const el = document.getElementById(id);
  return el ? String(el.value || "").trim() : "";
}

const subjectTreeNodeLookup = new Map();
let subjectTreeExpandedIds = new Set();
subjectTreeSelectedFlow = ["company_success"];

function getAllSubjectTreeIds(nodes = getSubjectTreeRoots(), target = new Set()) {
  (Array.isArray(nodes) ? nodes : []).forEach((node) => {
    if (!node || !node.id) return;
    target.add(String(node.id));
    getAllSubjectTreeIds(Array.isArray(node.children) ? node.children : [], target);
  });
  return target;
}

function registerSubjectTreeNodes(nodes = []) {
  nodes.forEach((node) => {
    if (!node || !node.id) return;

    subjectTreeNodeLookup.set(String(node.id), node);
    registerSubjectTreeNodes(Array.isArray(node.children) ? node.children : []);
  });
}

registerSubjectTreeNodes(Array.isArray(kpiSubjectHierarchy) ? kpiSubjectHierarchy : []);

function getSubjectTreeRoots() {
  return Array.isArray(kpiSubjectHierarchy) ? kpiSubjectHierarchy : [];
}

function getSubjectNode(nodeId) {
  return nodeId ? subjectTreeNodeLookup.get(String(nodeId)) || null : null;
}

function getSubjectNodeParent(nodeId) {
  const node = getSubjectNode(nodeId);
  return node && node.parent_id ? getSubjectNode(node.parent_id) : null;
}

function getSubjectNodeAncestors(nodeId) {
  const ancestors = [];
  let currentNode = getSubjectNode(nodeId);

  while (currentNode && currentNode.parent_id) {
    const parentNode = getSubjectNode(currentNode.parent_id);
    if (!parentNode) break;
    ancestors.unshift(parentNode);
    currentNode = parentNode;
  }

  return ancestors;
}

function getDefaultSubjectTreeExpandedIds() {
  return getAllSubjectTreeIds();
}

function updateSubjectTreeCurrentPath(node = null) {
  const currentPathEl = document.getElementById("subjectTreeCurrentPath");
  if (!currentPathEl) return;

  const pathText = node?.full_path || getFieldValue("subject_path_display") || "";
  currentPathEl.textContent = pathText
    ? "Selected path: " + pathText
    : "Selected path: No subject selected yet.";
}

function updateSubjectTreeTrigger(node = null, fallbackLabel = "") {
  const labelEl = document.getElementById("subjectTreeLabel");
  const metaEl = document.getElementById("subjectTreeMeta");
  const chevronEl = document.getElementById("subjectTreeChevron");
  const treeSelect = document.getElementById("subjectTreeSelect");

  if (labelEl) {
    labelEl.textContent = node?.name || fallbackLabel || "Select subject from tree";
  }

  if (metaEl) {
    metaEl.textContent = node?.full_path || "Open the visual hierarchy to choose a subject and follow its branch.";
  }

  updateSubjectTreeCurrentPath(node);

  if (chevronEl && treeSelect) {
    chevronEl.textContent = treeSelect.classList.contains("open") ? "▴" : "▾";
  }
}

function toggleSubjectTree(forceOpen) {
  const treeSelect = document.getElementById("subjectTreeSelect");
  if (!treeSelect) return;

  const shouldOpen = typeof forceOpen === "boolean"
    ? forceOpen
    : !treeSelect.classList.contains("open");

  treeSelect.classList.toggle("open", shouldOpen);
  updateSubjectTreeTrigger(getSubjectNode(getFieldValue("subject_node_id")));
}

function closeSubjectTree() {
  toggleSubjectTree(false);
}

function getSubjectFlowColumns() {
  var roots = getSubjectTreeRoots();
  var columns = [roots];
  var currentNodes = roots;

  subjectTreeSelectedFlow.forEach(function(selectedId) {
    var selectedNode = null;
    for (var i = 0; i < currentNodes.length; i++) {
      if (String(currentNodes[i].id) === String(selectedId)) {
        selectedNode = currentNodes[i];
        break;
      }
    }
    if (selectedNode && Array.isArray(selectedNode.children) && selectedNode.children.length) {
      columns.push(selectedNode.children);
      currentNodes = selectedNode.children;
    }
  });

  return columns;
}

function renderSubjectFlowColumn(nodes = [], level = 0) {
  return ''
    + '<div class="subject-flow-column' + (subjectTreeSelectedFlow[level] ? ' is-active' : '') + '">'
    +   '<div class="subject-flow-column-title">'
    +     ('Level ' + (level + 1) + ' · ' + (level === 0 ? 'Parent subjects' : 'Related subjects'))
    +   '</div>'
    +   nodes.map((node) => {
          const nodeId = String(node.id || '');
          const isSelected = subjectTreeSelectedFlow[level] === nodeId;
          const hasChildren = Array.isArray(node.children) && node.children.length > 0;
          const metaText = hasChildren ? node.children.length + ' children' : 'Leaf';

          return ''
            + '<button type="button"'
            + ' class="subject-flow-card' + (isSelected ? ' selected' : '') + '"'
            + ' onclick="selectSubjectFlowNode(&quot;' + escapeHtml(nodeId) + '&quot;, ' + level + ')">'
            +   '<span class="subject-flow-kicker">L' + level + ' · ' + escapeHtml(metaText) + '</span>'
            +   '<span class="subject-flow-name">' + escapeHtml(node.name || '') + '</span>'
            +   '<span class="subject-flow-path">' + escapeHtml(node.parent_name || node.full_path || '') + '</span>'
            + '</button>';
        }).join('')
    + '</div>';
}
   
let currentTreeSearch = '';

function renderSubjectTree() {
  var treeList = document.getElementById('subjectTreeList');
  var statusEl = document.getElementById('subjectTreeFitMeta');
  if (!treeList) return;

  var columns = getSubjectFlowColumns();
  var search = (currentTreeSearch || '').toLowerCase().trim();

  var levelTitles = ['Parent subjects', 'Related subjects', 'Sub-category', 'KPI subjects'];
  var badgeClasses = ['sf-badge-l0', 'sf-badge-l1', 'sf-badge-l2', 'sf-badge-l3'];

  var html = '<div class="sf-flow">';

  columns.forEach(function(nodes, colIndex) {
    var selectedId = subjectTreeSelectedFlow[colIndex] || '';
    var levelTitle = levelTitles[colIndex] || ('Level ' + (colIndex + 1));
    var badgeClass = badgeClasses[colIndex] || 'sf-badge-lx';

    var filteredNodes = search
      ? nodes.filter(function(n) { return nodeMatchesSearch(n, search); })
      : nodes;

    var itemsHtml = filteredNodes.map(function(node) {
      var nodeId = String(node.id || '');
      var hasChildren = Array.isArray(node.children) && node.children.length > 0;
      var isSelected = selectedId === nodeId;
      var childCount = hasChildren ? node.children.length : 0;
      var metaText = hasChildren
        ? childCount + ' ' + (childCount === 1 ? 'child' : 'children')
        : 'Leaf · select this';

      return '<button type="button"' +
        ' class="sf-item' + (isSelected ? ' sf-selected' : '') + '"' +
        ' onclick="selectSubjectFlowNode(\\'' + escapeHtml(nodeId) + '\\', ' + colIndex + ')">' +
        '<span class="sf-badge ' + badgeClass + '">L' + colIndex + '</span>' +
        '<span class="sf-item-body">' +
          '<span class="sf-item-name">' + (search ? highlightText(node.name || '', currentTreeSearch) : escapeHtml(node.name || '')) + '</span>' +
          '<span class="sf-item-meta">' + escapeHtml(metaText) + '</span>' +
        '</span>' +
        '<span class="sf-arrow' + (hasChildren ? '' : ' sf-arrow-hidden') + '">&#9654;</span>' +
      '</button>';
    }).join('');

    if (!itemsHtml) {
      itemsHtml = '<div style="padding:14px 10px;font-size:11px;color:#94a3b8;text-align:center;">No matches</div>';
    }

    html += '<div class="sf-column">' +
      '<div class="sf-col-head">' +
        '<span class="sf-col-level">Level ' + (colIndex + 1) + '</span>' +
        '<span class="sf-col-title">' + escapeHtml(levelTitle) + '</span>' +
        '<span class="sf-col-count">' + filteredNodes.length + ' items</span>' +
      '</div>' +
      '<div class="sf-col-body">' + itemsHtml + '</div>' +
    '</div>';
  });

  html += '</div>';
  treeList.innerHTML = html;

  if (statusEl) {
    statusEl.textContent = columns.length + ' level' + (columns.length !== 1 ? 's' : '');
  }

  // Scroll selected item into view
  setTimeout(function() {
    var sel = treeList.querySelector('.sf-selected');
    if (sel) { sel.scrollIntoView({ block: 'nearest', behavior: 'smooth' }); }
  }, 60);
}


function setSubjectSelectedFlow(nodeId) {
  var node = getSubjectNode(nodeId);
  if (!node) return;
  var ancestorIds = getSubjectNodeAncestors(node.id).map(function(a) { return String(a.id); });
  subjectTreeSelectedFlow = ancestorIds.concat(String(node.id));
}

function selectSubjectFlowNode(nodeId, level) {
  var node = getSubjectNode(nodeId);
  if (!node) return;

  subjectTreeSelectedFlow = subjectTreeSelectedFlow.slice(0, level);
  subjectTreeSelectedFlow[level] = String(nodeId);

  var hasChildren = Array.isArray(node.children) && node.children.length > 0;
  selectSubjectNode(node.id, { keepOpen: true });
}

function resetSubjectFlow() {
  subjectTreeSelectedFlow = [];
  var searchEl = document.getElementById('subjectTreeSearch');
  if (searchEl) { searchEl.value = ''; }
  currentTreeSearch = '';
  renderSubjectTree();
}

function onSubjectTreeSearch(term) {
  currentTreeSearch = term.trim();
  renderSubjectTree();
}

function expandAllSubjectTree() { renderSubjectTree(); }
function collapseAllSubjectTree() { renderSubjectTree(); }
function toggleSubjectTreeNode() { renderSubjectTree(); }

function findSubjectNodeForValues(values = {}) {
  const subjectNodeId = normalizeNullableFieldValue(values.subject_node_id || values.subject_id);
  const subjectText = normalizeNullableFieldValue(values.subject);
  const categoryText = normalizeNullableFieldValue(values.indicator_title);
  const nameText = normalizeNullableFieldValue(values.indicator_sub_title);

  if (subjectNodeId) {
    const directNode = getSubjectNode(subjectNodeId);
    if (directNode) return directNode;
  }

  if (subjectText) {
    const directSubjectMatch = Array.from(subjectTreeNodeLookup.values()).find((node) =>
      String(node.full_path || "") === subjectText || String(node.name || "") === subjectText
    );
    if (directSubjectMatch) return directSubjectMatch;
  }

  if (nameText && categoryText) {
    const matches = Array.from(subjectTreeNodeLookup.values()).filter((node) => String(node.name || "") === nameText);
    const parentMatch = matches.find((node) => {
      const parentNode = getSubjectNodeParent(node.id);
      return parentNode && (
        String(parentNode.name || "") === categoryText ||
        String(parentNode.full_path || "") === categoryText
      );
    });

    if (parentMatch) return parentMatch;

    const pathMatch = matches.find((node) => String(node.full_path || "").includes(categoryText));
    if (pathMatch) return pathMatch;
  }

  if (nameText) {
    const nameMatches = Array.from(subjectTreeNodeLookup.values()).filter((node) => String(node.name || "") === nameText);
    if (nameMatches.length === 1) {
      return nameMatches[0];
    }
  }

  return null;
}

function selectSubjectNode(nodeId, { preserveName = false, preserveSubtitle = false, preserveDefinition = false, fallbackDefinition = "", keepOpen = true } = {}) {
  const node = getSubjectNode(nodeId);
  if (!node) return;

  const parentNode = getSubjectNodeParent(node.id);
  setSubjectSelectedFlow(node.id);

  const subjectInput = document.getElementById("subject");
  const subjectNodeInput = document.getElementById("subject_node_id");
  const categoryInput = document.getElementById("indicator_title");
  const nameInput = document.getElementById("indicator_sub_title");
  const pathDisplayInput = document.getElementById("subject_path_display");
  const definitionEl = document.getElementById("definition");

  if (subjectInput) subjectInput.value = node.full_path || node.name || "";
  if (subjectNodeInput) subjectNodeInput.value = node.id || "";
  if (categoryInput && (!preserveSubtitle || !String(categoryInput.value || "").trim())) {
    categoryInput.value = parentNode ? parentNode.name || "" : node.name || "";
  }
  if (pathDisplayInput) pathDisplayInput.value = node.full_path || node.name || "";



  if (definitionEl) {
    definitionEl.value = preserveDefinition
      ? (fallbackDefinition || node.description || "")
      : (node.description || fallbackDefinition || "");
  }

  updateSubjectTreeTrigger(node);
  renderSubjectTree();
  toggleSubjectTree(keepOpen);
  updateModalOverview();
}

function selectSubjectTreeNode(nodeId) {
  const node = getSubjectNode(nodeId);
  if (!node) return;

  const hasChildren = Array.isArray(node.children) && node.children.length > 0;
  selectSubjectNode(node.id, { keepOpen: hasChildren });
}

function initializeKpiHierarchySelectors(values = {}) {
  subjectTreeSelectedFlow = [];

  const subjectInput = document.getElementById("subject");
  const subjectNodeInput = document.getElementById("subject_node_id");
  const categoryInput = document.getElementById("indicator_title");
  const nameInput = document.getElementById("indicator_sub_title");
  const pathDisplayInput = document.getElementById("subject_path_display");
  const definitionEl = document.getElementById("definition");

  if (subjectInput) subjectInput.value = "";
  if (subjectNodeInput) subjectNodeInput.value = "";
  if (categoryInput) categoryInput.value = "";
  if (pathDisplayInput) pathDisplayInput.value = "";
  if (nameInput) nameInput.value = values.indicator_sub_title || "";
  if (definitionEl) definitionEl.value = values.definition || "";

  const matchedNode = findSubjectNodeForValues(values);
  if (matchedNode) {
    selectSubjectNode(matchedNode.id, {
      preserveName: true,
      preserveSubtitle: true,
      preserveDefinition: true,
      fallbackDefinition: values.definition || "",
      keepOpen: false
    });
  } else {
    if (subjectInput) subjectInput.value = values.subject || "";
    if (categoryInput) categoryInput.value = values.indicator_title || "";
    if (pathDisplayInput) pathDisplayInput.value = values.subject || "";
    updateSubjectTreeTrigger(null, values.subject || "");
    renderSubjectTree();
    closeSubjectTree();
  }
}

function formatReferenceKpiLabel(row) {
  if (!row) return "";

  const kpiName = String(row.indicator_sub_title || "").trim();

  return  (kpiName ? " " + kpiName : "");
}

function getReferenceKpiCandidates() {
  const currentKpiId = getFieldValue("kpi_id");

  return (referenceKpiRows || []).filter(row =>
    row &&
    row.kpi_id !== undefined &&
    row.kpi_id !== null &&
    String(row.kpi_id) !== String(currentKpiId)
  );
}

function getReferenceKpiRow(referenceKpiId) {
  return (referenceKpiRows || []).find(row => String(row.kpi_id) === String(referenceKpiId)) || null;
}

function populateReferenceKpiOptions(selectedValue = "") {
  const selectEl = document.getElementById("reference_kpi_id");
  if (!selectEl) return;

  const selectedText = String(selectedValue || "").trim();
  const candidates = getReferenceKpiCandidates();
  const options = candidates.map(row => ({
    value: String(row.kpi_id),
    label: formatReferenceKpiLabel(row)
  }));

  if (selectedText && !options.some(option => option.value === selectedText)) {
    options.push({
      value: selectedText,
      label: "#" + selectedText
    });
  }

  selectEl.innerHTML =
    '<option value="">Select reference KPI</option>' +
    options.map(option =>
      '<option value="' + escapeHtml(option.value) + '">' + escapeHtml(option.label) + '</option>'
    ).join("");

  selectEl.value = selectedText;
  selectEl.disabled = options.length === 0;
}

function updateDisplayedKpiPreview() {
  const previewEl = document.getElementById("displayed_kpi_preview");
  if (!previewEl) return;

  const referenceKpiId = getFieldValue("reference_kpi_id");
  const referenceRow = referenceKpiId ? getReferenceKpiRow(referenceKpiId) : null;
  const referenceLabel = referenceRow
  ? String(referenceRow.indicator_sub_title || "").trim()
  : "";

  previewEl.value = referenceLabel
    ? "Displayed KPI = Raw value / " + referenceLabel + " x 100"
    : "Displayed KPI = Raw value / Reference KPI x 100";
}

function handleCalculationModeChange() {
  const mode = getFieldValue("calculation_mode").toLowerCase();
  const isRatio = mode === "ratio";

  const referenceWrap = document.getElementById("wrap_reference_kpi_id");
  const displayedWrap = document.getElementById("wrap_displayed_kpi_preview");
  const helpWrap = document.getElementById("wrap_ratio_mode_help");

  if (referenceWrap) referenceWrap.style.display = isRatio ? "" : "none";
  if (displayedWrap) displayedWrap.style.display = isRatio ? "" : "none";
  if (helpWrap) helpWrap.style.display = isRatio ? "" : "none";

  if (isRatio) {
    populateReferenceKpiOptions(getFieldValue("reference_kpi_id"));
    updateDisplayedKpiPreview();
  } else {
    const previewEl = document.getElementById("displayed_kpi_preview");
    if (previewEl) previewEl.value = "";
  }
}

      function setActiveNav(target) {
        document.getElementById("navDashboard").classList.remove("active");
        document.getElementById("navMyKpis").classList.remove("active");
        document.getElementById("navConsultAllocations").classList.remove("active");
        document.getElementById(target).classList.add("active");
      }

      function showDashboard() {
        document.getElementById("dashboardSection").style.display = "block";
        document.getElementById("myKpisSection").style.display = "none";
        document.getElementById("consultTargetAllocationsSection").style.display = "none";
        setActiveNav("navDashboard");
      }

      async function showMyKpis() {
        document.getElementById("dashboardSection").style.display = "none";
        document.getElementById("myKpisSection").style.display = "block";
        document.getElementById("consultTargetAllocationsSection").style.display = "none";
        setActiveNav("navMyKpis");

        if (!chartsLoaded) {
          await loadKpiCharts();
          chartsLoaded = true;
        }
      }

      async function showConsultTargetAllocations() {
        document.getElementById("dashboardSection").style.display = "none";
        document.getElementById("myKpisSection").style.display = "none";
        document.getElementById("consultTargetAllocationsSection").style.display = "block";
        setActiveNav("navConsultAllocations");

        if (!parameterSourceRows.length) {
          await loadParameterKpis();
        } else {
          applyParameterSearch();
        }
      }

      function updateStats(rows) {
        const total = rows.length;
        const monthly = rows.filter(r => String(r.frequency || "").toLowerCase().includes("month")).length;
        const withTarget = rows.filter(r => r.target !== null && r.target !== undefined && String(r.target).trim() !== "").length;
        const tolerance = rows.filter(r => r.tolerance_type && String(r.tolerance_type).trim() !== "").length;

        document.getElementById("statTotal").textContent = total;
        document.getElementById("statMonthly").textContent = monthly;
        document.getElementById("statTarget").textContent = withTarget;
        document.getElementById("statTolerance").textContent = tolerance;
      }

function getLastPathSegment(pathText) {
  const normalizedText = String(pathText || "").trim();
  if (!normalizedText) return "";

  const parts = normalizedText
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);

  return parts.length ? parts[parts.length - 1] : normalizedText;
}

function renderKpis(rows) {
  currentRows = rows || [];
  updateStats(currentRows);

  const grid = document.getElementById("grid");

  if (!currentRows.length) {
    grid.innerHTML = '<div class="empty">No KPI found for this responsible.</div>';
    return;
  }

  grid.innerHTML = \`
    <div class="kpi-table-shell">
      <div class="kpi-table-wrap">
        <table class="kpi-table">
          <thead>
            <tr>
              <th>Category</th>
              <th>KPI</th>
              <th class="kpi-table-actions"></th>
            </tr>
          </thead>
          <tbody>
            \${currentRows.map(row => \`
              <tr>
                <td>
                  <div class="kpi-row-title">\${escapeHtml(getLastPathSegment(row.full_subject_path || row.subject || row.indicator_title) || "Untitled KPI")}</div>
                  <div class="allocation-row-meta">\${escapeHtml([
                    row.full_subject_path || row.subject || "",
                    row.set_by_people_display_name ? 'Responsible: ' + row.set_by_people_display_name : ''
                  ].filter(Boolean).join(" • ") || "No responsible assignment yet")}</div>
                </td>
                <td>
                  <div class="kpi-row-subtitle">\${escapeHtml(row.indicator_sub_title || "-")}</div>
                  <div class="allocation-row-meta">\${escapeHtml([
                    row.frequency ? 'Frequency: ' + row.frequency : '',
                    row.target !== null && row.target !== undefined && String(row.target).trim() !== ''
                      ? 'Target: ' + formatParameterDisplayValue(row.target) + (row.allocation_target_unit || row.unit ? ' ' + (row.allocation_target_unit || row.unit) : '')
                      : '',
                    row.status ? 'Status: ' + row.status : ''
                  ].filter(Boolean).join(" • ") || "No KPI details")}</div>
                </td>
              <td class="kpi-table-actions">
  <button type="button" class="action-btn edit-btn" onclick="openEditModal(\${row.kpi_id})">Edit KPI</button>
  <button type="button" class="action-btn delete-btn" onclick="deleteKpi(\${row.kpi_id})">Delete</button>
</td>
              </tr>
            \`).join("")}
          </tbody>
        </table>
      </div>
    </div>
  \`;
}

      function getParameterSearchValue() {
        const searchEl = document.getElementById("parameterSearch");
        return searchEl ? String(searchEl.value || "") : "";
      }

      function buildParameterSearchIndex(row) {
        return [
          row.kpi_name,
          row.kpi_subject,
          row.kpi_group,
          row.kpi_type,
          row.target_status,
          row.plant_name,
          row.zone_name,
          row.unit_name,
          row.function,
          row.product_line_name,
          row.product_name,
          row.market_name,
          row.customer_name,
          row.target_unit,
          row.local_currency,
          formatParameterDisplayValue(row.target_value),
          formatParameterDisplayDate(row.target_setup_date)
        ]
          .filter(Boolean)
          .join(" ")
          .toLowerCase();
      }

      function filterParameterRows(rows, search = "") {
        const sourceRows = Array.isArray(rows) ? rows : [];
        const query = String(search || "").trim().toLowerCase();
        if (!query) return sourceRows;

        return sourceRows.filter((row) => buildParameterSearchIndex(row).includes(query));
      }

      function formatParameterDisplayValue(value) {
        if (value === null || value === undefined || value === "") return "-";
        const numeric = Number(value);
        if (Number.isFinite(numeric)) {
          return numeric.toLocaleString("en-US", {
            minimumFractionDigits: Math.abs(numeric % 1) > 0.001 ? 2 : 0,
            maximumFractionDigits: 2
          });
        }
        return String(value);
      }

      function formatParameterDisplayDate(value) {
        if (!value) return "-";
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) return escapeHtml(value);
        return parsed.toLocaleDateString("en-GB", {
          day: "2-digit",
          month: "short",
          year: "numeric"
        });
      }

      function renderParameterKpis(rows) {
        currentParameterRows = rows || [];
        const parameterGrid = document.getElementById("parameterGrid");
        if (!parameterGrid) return;

        if (!currentParameterRows.length) {
          parameterGrid.innerHTML = '<div class="empty">No KPI target allocation found for the current search.</div>';
          return;
        }

        parameterGrid.innerHTML = \`
          <div class="kpi-table-shell">
            <div class="kpi-table-wrap">
              <table class="kpi-table allocation-table">
                <thead>
                  <tr>
                    <th>KPI</th>
                    <th>Target</th>
                    <th class="kpi-table-actions"></th>
                  </tr>
                </thead>
                <tbody>
                  \${currentParameterRows.map(row => \`
                    <tr>
                   
                      <td>
                        <div class="kpi-row-subtitle">\${escapeHtml(row.kpi_name || row.kpi_group || "Linked KPI")}</div>
                        <div class="allocation-row-meta">\${escapeHtml([row.kpi_group, row.target_status ? 'Status: ' + row.target_status : '', row.set_by_people_display_name ? 'Set by: ' + row.set_by_people_display_name : ''].filter(Boolean).join(" • ") || "No KPI metadata")}</div>
                      </td>
                      <td>
                        <div class="allocation-target-value">\${escapeHtml(formatParameterDisplayValue(row.target_value))} \${escapeHtml(row.target_unit || "")}</div>
                        <div class="allocation-row-meta">\${escapeHtml([
                          row.local_currency,
                          row.target_setup_date ? 'Setup: ' + formatParameterDisplayDate(row.target_setup_date) : '',
                          row.target_start_date ? 'Start: ' + formatParameterDisplayDate(row.target_start_date) : '',
                          row.target_end_date ? 'End: ' + formatParameterDisplayDate(row.target_end_date) : '',
                          row.approved_by_people_display_name ? 'Approved: ' + row.approved_by_people_display_name : ''
                        ].filter(Boolean).join(" • ") || "No target details")}</div>
                      </td>
                      <td class="kpi-table-actions">
                        <div class="table-actions-group">
                          <button type="button" class="action-btn edit-btn" onclick="openEditParameterModal(\${row.kpi_target_allocation_id})">Edit</button>
                          <button type="button" class="action-btn delete-btn" onclick="deleteParameterKpi(\${row.kpi_target_allocation_id})">Delete</button>
                        </div>
                      </td>
                    </tr>
                  \`).join("")}
                </tbody>
              </table>
            </div>
          </div>
        \`;
      }

      function applyParameterSearch(search = getParameterSearchValue()) {
        renderParameterKpis(filterParameterRows(parameterSourceRows, search));
      }

      async function loadParameterKpis(search = getParameterSearchValue()) {
        const parameterGrid = document.getElementById("parameterGrid");

        try {
          const res = await fetch('/api/responsibles/' + responsibleId + '/kpi-objects');
          const data = await res.json();

          if (!res.ok) {
            throw new Error(data?.error || "Failed to load KPI target allocations");
          }

          parameterSourceRows = Array.isArray(data) ? data : [];
          applyParameterSearch(search);
        } catch (error) {
          parameterSourceRows = [];
          if (parameterGrid) {
            parameterGrid.innerHTML = '<div class="empty">Failed to load KPI target allocations.</div>';
          }
        }
      }

      function normalizeChartLimitValue(value) {
        if (value === null || value === undefined || value === "") return null;
        const numeric = Number(value);
        return Number.isFinite(numeric) ? numeric : null;
      }

      function getChartValueBandColor(value, lowLimit, highLimit) {
        const numericValue = normalizeChartLimitValue(value);
        const low = normalizeChartLimitValue(lowLimit);
        const high = normalizeChartLimitValue(highLimit);

        if (numericValue === null) return "rgba(148, 163, 184, 0.85)";

        const lowerBound = low !== null && high !== null ? Math.min(low, high) : low;
        const upperBound = low !== null && high !== null ? Math.max(low, high) : high;

        if (upperBound !== null && numericValue > upperBound) {
          return "rgba(239, 68, 68, 0.88)";
        }

        if (lowerBound !== null && numericValue < lowerBound) {
          return "rgba(22, 163, 74, 0.92)";
        }

        return "rgba(74, 222, 128, 0.88)";
      }

      function getChartValueBorderColor(value, lowLimit, highLimit) {
        const color = getChartValueBandColor(value, lowLimit, highLimit);
        if (color.includes("239, 68, 68")) return "rgba(220, 38, 38, 1)";
        if (color.includes("22, 163, 74")) return "rgba(21, 128, 61, 1)";
        return "rgba(22, 163, 74, 1)";
      }

      const kpiThresholdBandsPlugin = {
        id: "kpiThresholdBands",
        beforeDatasetsDraw(chart, args, pluginOptions) {
          const low = normalizeChartLimitValue(pluginOptions?.lowLimit);
          const high = normalizeChartLimitValue(pluginOptions?.highLimit);
          const yScale = chart.scales?.y;
          const area = chart.chartArea;

          if (!yScale || !area || (low === null && high === null)) return;

          const lowerBound = low !== null && high !== null ? Math.min(low, high) : low;
          const upperBound = low !== null && high !== null ? Math.max(low, high) : high;

          const drawBand = (fromValue, toValue, fillStyle) => {
            if (fromValue === null || toValue === null) return;
            const startY = yScale.getPixelForValue(fromValue);
            const endY = yScale.getPixelForValue(toValue);
            if (!Number.isFinite(startY) || !Number.isFinite(endY)) return;

            const top = Math.min(startY, endY);
            const height = Math.abs(endY - startY);
            if (!height) return;

            chart.ctx.fillStyle = fillStyle;
            chart.ctx.fillRect(area.left, top, area.right - area.left, height);
          };

          chart.ctx.save();
          chart.ctx.beginPath();
          chart.ctx.rect(area.left, area.top, area.right - area.left, area.bottom - area.top);
          chart.ctx.clip();

          if (upperBound !== null) {
            drawBand(yScale.max, upperBound, "rgba(254, 226, 226, 0.55)");
          }

          if (lowerBound !== null && upperBound !== null) {
            drawBand(upperBound, lowerBound, "rgba(220, 252, 231, 0.55)");
          } else if (upperBound !== null) {
            drawBand(upperBound, yScale.min, "rgba(220, 252, 231, 0.55)");
          }

          if (lowerBound !== null) {
            drawBand(lowerBound, yScale.min, "rgba(187, 247, 208, 0.55)");
          }

          chart.ctx.restore();
        }
      };

      const kpiFlowLinePlugin = {
        id: "kpiFlowLine",
        afterDatasetsDraw(chart, args, pluginOptions) {
          const datasetIndex = Number.isInteger(pluginOptions?.datasetIndex)
            ? pluginOptions.datasetIndex
            : 0;
          const meta = chart.getDatasetMeta(datasetIndex);
          const area = chart.chartArea;

          if (!meta?.data?.length || !area) return;

          const offset = Number.isFinite(pluginOptions?.offset) ? pluginOptions.offset : 12;
          const pointRadius = Number.isFinite(pluginOptions?.pointRadius) ? pluginOptions.pointRadius : 3.5;
          const points = [];

          meta.data.forEach((barElement, index) => {
            const rawValue = chart.data?.datasets?.[datasetIndex]?.data?.[index];
            const numericValue = Number(rawValue);

            if (!Number.isFinite(numericValue) || !Number.isFinite(barElement?.x) || !Number.isFinite(barElement?.y)) {
              return;
            }

            points.push({
              x: barElement.x,
              y: Math.max(barElement.y - offset, area.top + 8)
            });
          });

          if (points.length < 2) return;

          const ctx = chart.ctx;
          const strokeStyle = pluginOptions?.color || "rgba(15, 23, 42, 0.92)";
          const pointFill = pluginOptions?.pointFill || "rgba(15, 23, 42, 0.96)";
          const pointStroke = pluginOptions?.pointStroke || "rgba(255, 255, 255, 0.98)";
          const lineWidth = Number.isFinite(pluginOptions?.lineWidth) ? pluginOptions.lineWidth : 3;

          ctx.save();
          ctx.lineWidth = lineWidth;
          ctx.strokeStyle = strokeStyle;
          ctx.lineCap = "round";
          ctx.lineJoin = "round";
          ctx.beginPath();
          ctx.moveTo(points[0].x, points[0].y);

          for (let index = 1; index < points.length; index += 1) {
            const previousPoint = points[index - 1];
            const currentPoint = points[index];
            const midX = (previousPoint.x + currentPoint.x) / 2;
            const midY = (previousPoint.y + currentPoint.y) / 2;
            ctx.quadraticCurveTo(previousPoint.x, previousPoint.y, midX, midY);
          }

          const lastPoint = points[points.length - 1];
          ctx.lineTo(lastPoint.x, lastPoint.y);
          ctx.stroke();

          points.forEach((point) => {
            ctx.beginPath();
            ctx.fillStyle = pointFill;
            ctx.arc(point.x, point.y, pointRadius, 0, Math.PI * 2);
            ctx.fill();

            ctx.lineWidth = 1.5;
            ctx.strokeStyle = pointStroke;
            ctx.stroke();
            ctx.lineWidth = lineWidth;
            ctx.strokeStyle = strokeStyle;
          });

          ctx.restore();
        }
      };

      async function loadKpis(search = "") {
        try {
          const res = await fetch('/api/responsibles/' + responsibleId + '/kpis?search=' + encodeURIComponent(search));
          const data = await res.json();
          renderKpis(data);
        } catch (error) {
          document.getElementById("grid").innerHTML =
            '<div class="empty">Failed to load KPIs.</div>';
        }
      }

      async function loadReferenceKpis() {
        try {
          const res = await fetch('/api/responsibles/' + responsibleId + '/kpis?search=');
          const data = await res.json();
          referenceKpiRows = Array.isArray(data) ? data : [];
        } catch (error) {
          referenceKpiRows = Array.isArray(currentRows) ? currentRows.slice() : [];
        }
      }

      async function loadKpiCharts() {
        const chartsGrid = document.getElementById("chartsGrid");

        try {
          const res = await fetch('/api/responsibles/' + responsibleId + '/kpi-history-monthly');
          if (!res.ok) throw new Error("Failed to load chart data");

          const rows = await res.json();

          if (!rows.length) {
            chartsGrid.innerHTML = '<div class="empty">No KPI history found for this responsible.</div>';
            return;
          }

          chartsGrid.innerHTML = rows.map((row, index) =>
            '<div class="chart-card">' +
              '<h3>' + escapeHtml(row.indicator_title || "Untitled KPI") + '</h3>' +
              '<p>' + escapeHtml(row.indicator_sub_title || "Monthly KPI history") + '</p>' +
              '<div class="chart-meta">' +
                '<div class="chart-chip chart-chip-target">Target: ' + escapeHtml(row.targetValue ?? "-") + '</div>' +
                '<div class="chart-chip chart-chip-high">High Limit: ' + escapeHtml(row.highLimitValue ?? "-") + '</div>' +
                '<div class="chart-chip chart-chip-low">Low Limit: ' + escapeHtml(row.lowLimitValue ?? "-") + '</div>' +
              '</div>' +
              '<div class="chart-wrap">' +
                '<canvas id="chart_' + index + '"></canvas>' +
              '</div>' +
            '</div>'
          ).join("");

          chartInstances.forEach(chart => chart.destroy());
          chartInstances = [];

          rows.forEach((row, index) => {
            const canvas = document.getElementById('chart_' + index);
            const ctx = canvas.getContext('2d');
            const sourceLabels = Array.isArray(row.labels) ? row.labels : [];
            const periodLabels = sourceLabels.map((_, labelIndex) => 'Period ' + (labelIndex + 1));

            const targetSeries = Array.isArray(sourceLabels)
              ? sourceLabels.map(() => row.targetValue ?? null)
              : [];

            const highLimitSeries = Array.isArray(sourceLabels)
              ? sourceLabels.map(() => row.highLimitValue ?? null)
              : [];

            const lowLimitSeries = Array.isArray(sourceLabels)
              ? sourceLabels.map(() => row.lowLimitValue ?? null)
              : [];
            const barColors = Array.isArray(row.values)
              ? row.values.map(value => getChartValueBandColor(value, row.lowLimitValue, row.highLimitValue))
              : [];
            const barBorderColors = Array.isArray(row.values)
              ? row.values.map(value => getChartValueBorderColor(value, row.lowLimitValue, row.highLimitValue))
              : [];
            const axisValues = [
              ...(Array.isArray(row.values) ? row.values : []),
              ...targetSeries,
              ...highLimitSeries,
              ...lowLimitSeries
            ]
              .filter((value) => value !== null && value !== undefined && value !== '')
              .map((value) => Number(value))
              .filter((value) => Number.isFinite(value));
            const axisSourceMin = axisValues.length ? Math.min(...axisValues) : 0;
            const axisSourceMax = axisValues.length ? Math.max(...axisValues) : 100;
            let axisMin = axisSourceMin > 0
              ? axisSourceMin * 0.8
              : axisSourceMin < 0
                ? axisSourceMin * 1.2
                : 0;
            let axisMax = axisSourceMax > 0
              ? axisSourceMax * 1.2
              : axisSourceMax < 0
                ? axisSourceMax * 0.8
                : 0;
            if (axisSourceMin === axisSourceMax) {
              const pad = Math.max(Math.abs(axisSourceMax || axisSourceMin || 1) * 0.2, 1);
              axisMin = axisSourceMin - pad;
              axisMax = axisSourceMax + pad;
            }

            const chart = new Chart(ctx, {
              data: {
                labels: periodLabels,
                datasets: [
                  {
                    type: 'bar',
                    label: 'Actual Value',
                    data: row.values,
                    borderWidth: 1,
                    borderColor: barBorderColors,
                    backgroundColor: barColors,
                    borderRadius: 6,
                    borderSkipped: false,
                    barThickness: 26
                  },
                  {
                    type: 'line',
                    label: 'Target',
                    data: targetSeries,
                    borderColor: 'rgba(239, 68, 68, 0.95)',
                    backgroundColor: 'rgba(239, 68, 68, 0.15)',
                    borderWidth: 2,
                    tension: 0,
                    fill: false,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    borderDash: [6, 4]
                  },
                  {
                    type: 'line',
                    label: 'High Limit',
                    data: highLimitSeries,
                    borderColor: 'rgba(245, 158, 11, 0.95)',
                    backgroundColor: 'rgba(245, 158, 11, 0.15)',
                    borderWidth: 2,
                    tension: 0,
                    fill: false,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    borderDash: [8, 5]
                  },
                  {
                    type: 'line',
                    label: 'Low Limit',
                    data: lowLimitSeries,
                    borderColor: 'rgba(59, 130, 246, 0.95)',
                    backgroundColor: 'rgba(59, 130, 246, 0.15)',
                    borderWidth: 2,
                    tension: 0,
                    fill: false,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    borderDash: [8, 5]
                  }
                ]
              },
              options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                  kpiThresholdBands: {
                    lowLimit: row.lowLimitValue,
                    highLimit: row.highLimitValue
                  },
                  kpiFlowLine: {
                    datasetIndex: 0,
                    offset: 12,
                    lineWidth: 3,
                    pointRadius: 3.5
                  },
                  legend: {
                    display: false
                  },
                  tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                      title(items) {
                        const indexValue = items?.[0]?.dataIndex ?? 0;
                        return periodLabels[indexValue] || '';
                      },
                      footer(items) {
                        const indexValue = items?.[0]?.dataIndex ?? 0;
                        return sourceLabels[indexValue] ? 'Source: ' + sourceLabels[indexValue] : '';
                      }
                    }
                  }
                },
                interaction: {
                  mode: 'nearest',
                  axis: 'x',
                  intersect: false
                },
                scales: {
                  x: {
                    title: {
                      display: true,
                      text: 'Period',
                      color: '#0f172a',
                      font: {
                        size: 13,
                        weight: '700'
                      }
                    },
                    grid: {
                      display: false
                    },
                    ticks: {
                      color: '#64748b'
                    }
                  },
                  y: {
                    beginAtZero: false,
                    min: axisMin,
                    max: axisMax,
                    title: {
                      display: true,
                      text: 'Value',
                      color: '#0f172a',
                      font: {
                        size: 13,
                        weight: '700'
                      }
                    },
                    ticks: {
                      color: '#64748b'
                    },
                    grid: {
                      color: 'rgba(148,163,184,0.15)'
                    }
                  }
                }
              },
              plugins: [kpiThresholdBandsPlugin, kpiFlowLinePlugin]
            });

            chartInstances.push(chart);
          });
        } catch (error) {
          console.error(error);
          chartsGrid.innerHTML = '<div class="empty">Failed to load KPI charts.</div>';
        }
      }

      async function refreshKpis() {
        await loadKpis(document.getElementById("search").value || "");
        await loadParameterKpis();
        showToast("Dashboard refreshed");
      }

      function openModal() {
        document.getElementById("modalBackdrop").classList.add("open");
      }

      function closeModal() {
        document.getElementById("modalBackdrop").classList.remove("open");
      }

      function openParameterModal() {
        document.getElementById("parameterModalBackdrop").classList.add("open");
      }

      function closeParameterModal() {
        document.getElementById("parameterModalBackdrop").classList.remove("open");
      }

      function getParameterFieldValue(id) {
        const element = document.getElementById(id);
        return element ? String(element.value || "").trim() : "";
      }

      function updateParameterOverview() {
   
      }

      function getFieldValue(id) {
        const element = document.getElementById(id);
        return element ? String(element.value || "").trim() : "";
      }

      function updateModalOverview() {
        const isEdit = !!getFieldValue("kpi_id");
  
      }

      function normalizeLimitInput(value, allowPercent = false) {
        if (value === null || value === undefined) return null;
        let normalized = String(value).trim();
        if (!normalized) return null;

        normalized = normalized.replace(/\\s+/g, "").replace(/,/g, ".");
        if (allowPercent && normalized.endsWith("%")) {
          normalized = normalized.slice(0, -1);
        }

        return /^[+-]?(?:\\d+\\.?\\d*|\\.\\d+)$/.test(normalized) ? normalized : null;
      }

      function parseLimitNumber(value, allowPercent = false) {
        const normalized = normalizeLimitInput(value, allowPercent);
        return normalized === null ? null : Number(normalized);
      }

      function formatCalculatedLimit(value) {
        if (!Number.isFinite(value)) return "";
        return value.toFixed(10).replace(/\\.?0+$/, "");
      }

      function isRelativeToleranceType(toleranceType) {
        return String(toleranceType || "").trim().toLowerCase() === "relative";
      }

      function parseToleranceInputValue(value) {
        if (value === null || value === undefined) {
          return { text: "", numeric: null, hasPercent: false };
        }

        const text = String(value).trim();
        if (!text) {
          return { text: "", numeric: null, hasPercent: false };
        }

        const normalized = normalizeLimitInput(text, true);
        return {
          text,
          numeric: normalized === null ? null : Number(normalized),
          hasPercent: text.includes("%")
        };
      }

      function parseToleranceDelta(value, toleranceType, direction = "up") {
        const { numeric, hasPercent } = parseToleranceInputValue(value);
        if (!Number.isFinite(numeric)) return null;

        let delta = numeric;

        if (isRelativeToleranceType(toleranceType)) {
          delta = hasPercent || Math.abs(delta) > 1 ? delta / 100 : delta;
        }

        if (direction === "up") {
          return Math.abs(delta);
        }

        return delta > 0 ? -delta : delta;
      }

      function formatToleranceForInput(value, toleranceType, direction = "up") {
        const parsed = parseToleranceInputValue(value);
        if (!parsed.text) return "";
        if (!Number.isFinite(parsed.numeric)) return parsed.text;

        let displayValue = parsed.numeric;

        if (isRelativeToleranceType(toleranceType)) {
          displayValue = parsed.hasPercent || Math.abs(displayValue) > 1
            ? displayValue
            : displayValue * 100;
        }

        if (direction === "up") {
          displayValue = Math.abs(displayValue);
        } else if (displayValue > 0) {
          displayValue = -displayValue;
        }

        const suffix = isRelativeToleranceType(toleranceType) ? "%" : "";
        return formatCalculatedLimit(displayValue) + suffix;
      }

      function formatToleranceForDisplay(value, toleranceType, direction = "up") {
        return formatToleranceForInput(value, toleranceType, direction);
      }

      function serializeToleranceForPayload(value, toleranceType, direction = "up") {
        const parsed = parseToleranceInputValue(value);
        if (!parsed.text) return "";

        const delta = parseToleranceDelta(value, toleranceType, direction);
        return Number.isFinite(delta) ? formatCalculatedLimit(delta) : parsed.text;
      }

      function getLimitFields(source = null) {
        const root = source && typeof source.closest === "function"
          ? source.closest("#modalBackdrop") || source.closest(".modal") || document
          : document;

        return {
          targetInput: root.querySelector("#target") || document.getElementById("target"),
          toleranceTypeInput: root.querySelector("#tolerance_type") || document.getElementById("tolerance_type"),
          upToleranceInput: root.querySelector("#up_tolerance") || document.getElementById("up_tolerance"),
          lowToleranceInput: root.querySelector("#low_tolerance") || document.getElementById("low_tolerance"),
          highLimitInput: root.querySelector("#high_limit") || document.getElementById("high_limit"),
          lowLimitInput: root.querySelector("#low_limit") || document.getElementById("low_limit")
        };
      }

function syncToleranceInputs(source = null) {
    const {
      toleranceTypeInput,
      upToleranceInput,
      lowToleranceInput,
      highLimitInput,
      lowLimitInput
    } = getLimitFields(source);

    if (!toleranceTypeInput) return;

    const toleranceType = String(toleranceTypeInput.value || "").trim().toLowerCase();
    const isAbsolute = toleranceType === "absolute";
    const isRelative = toleranceType === "relative";

    // Show/hide wrapper divs
    const wrapUp   = document.getElementById("wrap_up_tolerance");
    const wrapLow  = document.getElementById("wrap_low_tolerance");
    const wrapHigh = document.getElementById("wrap_high_limit");
    const wrapLowL = document.getElementById("wrap_low_limit");

    if (wrapUp)   wrapUp.style.display   = isRelative ? "" : "none";
    if (wrapLow)  wrapLow.style.display  = isRelative ? "" : "none";
    if (wrapHigh) wrapHigh.style.display = isAbsolute ? "" : "none";
    if (wrapLowL) wrapLowL.style.display = isAbsolute ? "" : "none";

    // Clear hidden fields so stale values aren't submitted
    if (isAbsolute && upToleranceInput)  upToleranceInput.value  = "";
    if (isAbsolute && lowToleranceInput) lowToleranceInput.value = "";
    if (isRelative && highLimitInput)    highLimitInput.value    = "";
    if (isRelative && lowLimitInput)     lowLimitInput.value     = "";

    // Format tolerance display for Relative
    if (isRelative) {
      if (upToleranceInput && String(upToleranceInput.value || "").trim()) {
        upToleranceInput.value = formatToleranceForInput(upToleranceInput.value, toleranceType, "up");
      }
      if (lowToleranceInput && String(lowToleranceInput.value || "").trim()) {
        lowToleranceInput.value = formatToleranceForInput(lowToleranceInput.value, toleranceType, "low");
      }
    }
  }

      function handleToleranceTypeChange(source) {
        syncToleranceInputs(source);
        recalculateLimits(source);
      }

function recalculateLimits(sourceOrOptions = {}, maybeOptions = {}) {
  const source = sourceOrOptions && typeof sourceOrOptions.closest === "function"
    ? sourceOrOptions
    : null;

  const {
    targetInput,
    toleranceTypeInput,
    upToleranceInput,
    lowToleranceInput,
    highLimitInput,
    lowLimitInput
  } = getLimitFields(source);

  const toleranceType = String(toleranceTypeInput?.value || "").trim().toLowerCase();

  // Absolute = DO NOTHING
  if (toleranceType === "absolute") {
    return;
  }

  // Only Relative can calculate
  if (toleranceType !== "relative") {
    return;
  }

  const targetValue = parseLimitNumber(targetInput?.value);
  const upToleranceValue = parseToleranceDelta(upToleranceInput?.value, toleranceType, "up");
  const lowToleranceValue = parseToleranceDelta(lowToleranceInput?.value, toleranceType, "low");

  if (
    !Number.isFinite(targetValue) ||
    !Number.isFinite(upToleranceValue) ||
    !Number.isFinite(lowToleranceValue)
  ) {
    return;
  }

  highLimitInput.value = formatCalculatedLimit(targetValue * (1 + upToleranceValue));
  lowLimitInput.value = formatCalculatedLimit(targetValue * (1 + lowToleranceValue));
}

      function bindOverviewListeners() {
        ["frequency", "unit", "target"].forEach(id => {
          const el = document.getElementById(id);
          if (el) {
            el.addEventListener("input", updateModalOverview);
            el.addEventListener("change", updateModalOverview);
          }
        });
      }


      function bindHierarchyListeners() {
  const nameEl = document.getElementById("indicator_sub_title");
  const definitionEl = document.getElementById("definition");
  const treeTrigger = document.getElementById("subjectTreeTrigger");

  if (treeTrigger) {
    treeTrigger.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closeSubjectTree();
      }
    });
  }

  if (nameEl) {
    ["input", "change"].forEach((eventName) => {
      nameEl.addEventListener(eventName, () => {
        updateModalOverview();
      });
    });
  }

  if (definitionEl) {
    ["input", "change"].forEach((eventName) => {
      definitionEl.addEventListener(eventName, () => {
        updateModalOverview();
      });
    });
  }

  document.addEventListener("click", (event) => {
    const treeSelect = document.getElementById("subjectTreeSelect");
    if (!treeSelect) return;

    if (!treeSelect.contains(event.target)) {
      closeSubjectTree();
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeSubjectTree();
      updateModalOverview();
    }
  });
}
      function bindLimitListeners() {
        ["target", "up_tolerance", "low_tolerance"].forEach(id => {
          const el = document.getElementById(id);
          if (el) {
            el.addEventListener("input", (event) => recalculateLimits(event.target));
          }
        });

        const toleranceType = document.getElementById("tolerance_type");
        if (toleranceType) {
          toleranceType.addEventListener("change", (event) => handleToleranceTypeChange(event.target));
        }
      }

function handleCalculationOnChange() {
  const value = document.getElementById("calculation_on")?.value;
  const wrap = document.getElementById("wrap_nombre_periode");
  if (wrap) wrap.style.display = value === "Value" ? "" : "none";
  if (wrap && value !== "Value") {
    const select = document.getElementById("nombre_periode");
    if (select) select.value = "1 week";
  }
}

function handleDisplayTrendChange() {
  const value = document.getElementById("display_trend")?.value;
  const wrap = document.getElementById("wrap_regression");
  if (wrap) wrap.style.display = value === "Yes" ? "" : "none";
}

function normalizeNullableFieldValue(value) {
  const text = String(value ?? "").trim();
  if (!text) return "";

  const lowered = text.toLowerCase();
  if (lowered === "none" || lowered === "null" || lowered === "undefined" || lowered === "nan") {
    return "";
  }

  return text;
}

function inferValueType(value) {
  const text = normalizeNullableFieldValue(value);
  if (!text) return "Null";
  if (!Number.isNaN(Number(text)) && Number(text) === 0) return "0";
  return "Valeur a entrer";
}

function handleMinTypeChange() {
  const value = document.getElementById("min_type")?.value;
  const wrap = document.getElementById("wrap_min_value");
  const input = document.getElementById("min");

  if (wrap) wrap.style.display = value === "Valeur a entrer" ? "" : "none";
  if (!input) return;

  if (value === "Null") input.value = "";
  if (value === "0") input.value = "0";
  if (value === "Valeur a entrer" && input.value === "0") input.value = "";
}

function handleMaxTypeChange() {
  const value = document.getElementById("max_type")?.value;
  const wrap = document.getElementById("wrap_max_value");
  const input = document.getElementById("max");

  if (wrap) wrap.style.display = value === "Valeur a entrer" ? "" : "none";
  if (!input) return;

  if (value === "Null") input.value = "";
  if (value === "0") input.value = "0";
  if (value === "Valeur a entrer" && input.value === "0") input.value = "";
}


function resetForm() {
  [
    "kpi_id",
    "kpi_code",
    "kpi_formula",
    "unit",
    "definition",
    "frequency",
    "target",
    "tolerance_type",
    "up_tolerance",
    "low_tolerance",
    "max",
    "min",
    "calculation_on",
    "target_auto_adjustment",
    "high_limit",
    "low_limit",
    "direction",
    "nombre_periode",
    "calculation_mode",
    "reference_kpi_id",
    "display_trend",
    "regression",
    "min_type",
    "max_type",
    "status",
    "comments",
    "owner_role_id"
  ].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = "";
  });

  const toleranceTypeEl = document.getElementById("tolerance_type");
  if (toleranceTypeEl) toleranceTypeEl.value = "Relative";

  const displayTrendEl = document.getElementById("display_trend");
  if (displayTrendEl) displayTrendEl.value = "No";

  const calculationModeEl = document.getElementById("calculation_mode");
  if (calculationModeEl) calculationModeEl.value = "Direct";

  const minTypeEl = document.getElementById("min_type");
  if (minTypeEl) minTypeEl.value = "Null";

  const maxTypeEl = document.getElementById("max_type");
  if (maxTypeEl) maxTypeEl.value = "0";

  const statusEl = document.getElementById("status");
  if (statusEl) statusEl.value = "Active";

  handleCalculationOnChange();
  handleDisplayTrendChange();
  handleMinTypeChange();
  handleMaxTypeChange();
  populateReferenceKpiOptions("");
  populateRoleOptions("");
  handleCalculationModeChange();

  initializeKpiHierarchySelectors({
    subject: "",
    indicator_title: "",
    indicator_sub_title: "",
    definition: ""
  });
  updateModalOverview();
  syncToleranceInputs();
  recalculateLimits();
}

function setFieldValue(id, value) {
  const el = document.getElementById(id);
  if (!el) return;

  const finalValue = value ?? "";

  if (el.tagName === "SELECT" && finalValue !== "") {
    const exists = Array.from(el.options).some(opt => opt.value === String(finalValue));
    if (!exists) {
      el.insertAdjacentHTML(
        "beforeend",
        '<option value="' + escapeHtml(finalValue) + '">' + escapeHtml(finalValue) + '</option>'
      );
    }
  }

  el.value = finalValue;
}


function fillForm(data) {
  setFieldValue("kpi_id", data.kpi_id);

  initializeKpiHierarchySelectors({
    subject_node_id: data.subject_node_id || "",
    subject: data.subject || "",
    indicator_title: data.indicator_title || "",
    indicator_sub_title: data.indicator_sub_title || "",
    definition: data.definition || ""
  });

  setFieldValue("kpi_code", data.kpi_code);
  setFieldValue("kpi_formula", data.kpi_formula);
  setFieldValue("unit", data.unit);
  setFieldValue("definition", data.definition);
  setFieldValue("frequency", data.frequency);
  setFieldValue("direction", data.target_direction);
  setFieldValue("status", data.status || "Active");
  setFieldValue("comments", data.comments || "");

  setFieldValue("calculation_on", data.calculation_on);
  setFieldValue("nombre_periode", data.nombre_periode);
  setFieldValue("calculation_mode", data.calculation_mode || "Direct");
  populateReferenceKpiOptions(data.reference_kpi_id);
  populateRoleOptions(data.owner_role_id);
  setFieldValue("reference_kpi_id", data.reference_kpi_id);
  setFieldValue("owner_role_id", data.owner_role_id);
  setFieldValue("display_trend", data.display_trend);
  setFieldValue("regression", data.regression);

  setFieldValue("target", data.target);
  setFieldValue("target_auto_adjustment", data.target_auto_adjustment);

  setFieldValue("min_type", data.min_type || inferValueType(data.min));
  setFieldValue("max_type", data.max_type || inferValueType(data.max));
  setFieldValue("min", normalizeNullableFieldValue(data.min));
  setFieldValue("max", normalizeNullableFieldValue(data.max));

  setFieldValue("tolerance_type", data.tolerance_type || "Relative");
  setFieldValue("up_tolerance", formatToleranceForInput(data.up_tolerance, data.tolerance_type, "up"));
  setFieldValue("low_tolerance", formatToleranceForInput(data.low_tolerance, data.tolerance_type, "low"));
  setFieldValue("high_limit", data.high_limit);
  setFieldValue("low_limit", data.low_limit);

  handleCalculationOnChange();
  handleDisplayTrendChange();
  handleMinTypeChange();
  handleMaxTypeChange();
  handleCalculationModeChange();
  syncToleranceInputs();
  updateModalOverview();
}

      async function openCreateModal() {
        await loadReferenceKpis();
        resetForm();
        document.getElementById("modalTitle").textContent = "Add New KPI";
        document.getElementById("modalSubtitle").textContent =
          "Create a new KPI with clear identity, target logic and threshold visibility. It will be linked to " + (responsibleName || "this responsible") + " when you save its target allocation.";
        const deleteBtn = document.getElementById("deleteBtn");
        if (deleteBtn) {
         deleteBtn.style.display = "none";
        }
        updateModalOverview();
        openModal();
      }

      async function openEditModal(kpiId) {
        try {
          await loadReferenceKpis();
          const res = await fetch('/api/responsibles/' + responsibleId + '/kpis/' + kpiId);
          if (!res.ok) throw new Error("Failed to load KPI");

          const data = await res.json();
          fillForm(data);
          handleCalculationOnChange();
          handleDisplayTrendChange();
          handleMinTypeChange();
          handleMaxTypeChange();

          document.getElementById("modalTitle").textContent = "Edit KPI Attributes";
          document.getElementById("modalSubtitle").textContent =
            "";
          const deleteBtn = document.getElementById("deleteBtn");
          if (deleteBtn) {
           deleteBtn.style.display = "inline-flex";
          }

          updateModalOverview();
          openModal();
        } catch (error) {
        console.error("OPEN EDIT MODAL ERROR:", error);
        showToast("Unable to load KPI: " + error.message);
         } 
      }


        function getSafeValue(id, options = {}) {
      const optional = Boolean(options && options.optional);
      const el = document.getElementById(id);
     if (!el) {
      if (!optional) {
        console.error("Missing field:", id);
      }
    return "";
     }
     return el.value || "";
    }

      function buildPayload() {
        const toleranceType = document.getElementById("tolerance_type").value;
        const minType = document.getElementById("min_type")?.value || "Null";
        const maxType = document.getElementById("max_type")?.value || "Null";
        const minValue = normalizeNullableFieldValue(document.getElementById("min")?.value);
        const maxValue = normalizeNullableFieldValue(document.getElementById("max")?.value);
        return {
          indicator_title: getSafeValue("subject"),              // use subject as title
          indicator_sub_title: getSafeValue("indicator_sub_title"), // manual KPI name
          subject_node_id: getSafeValue("subject_node_id"),
          kpi_code: getSafeValue("kpi_code"),
          kpi_formula: getSafeValue("kpi_formula"),
          unit: getSafeValue("unit"),
          subject: getSafeValue("subject"),
          definition: getSafeValue("definition"),
          frequency: getSafeValue("frequency"),
          target: getSafeValue("target", { optional: true }),
          tolerance_type: toleranceType,
          up_tolerance: serializeToleranceForPayload(document.getElementById("up_tolerance").value, toleranceType, "up"),
          low_tolerance: serializeToleranceForPayload(document.getElementById("low_tolerance").value, toleranceType, "low"),
          max: maxType === "Null" ? "" : maxType === "0" ? "0" : maxValue,
          min: minType === "Null" ? "" : minType === "0" ? "0" : minValue,
          calculation_on: document.getElementById("calculation_on").value,
          target_auto_adjustment: document.getElementById("target_auto_adjustment").value,
          target_direction: document.getElementById("direction").value,
          nombre_periode: document.getElementById("nombre_periode").value,
          calculation_mode: document.getElementById("calculation_mode").value,
          reference_kpi_id: document.getElementById("calculation_mode").value === "Ratio"
            ? (document.getElementById("reference_kpi_id").value || null)
            : null,
          display_trend: document.getElementById("display_trend").value,
          regression: document.getElementById("regression").value,
          min_type: document.getElementById("min_type").value,
          max_type: document.getElementById("max_type").value,
          owner_role_id: document.getElementById("owner_role_id").value || null,
          status: document.getElementById("status").value,
          comments: document.getElementById("comments").value,
          high_limit: document.getElementById("high_limit").value || null,
          low_limit: document.getElementById("low_limit").value || null
        };
      }

      async function saveKpi() {
        const kpiId = document.getElementById("kpi_id").value;
        const isCreateMode = !kpiId;
        const method = kpiId ? "PUT" : "POST";
        const url = kpiId
          ? '/api/responsibles/' + responsibleId + '/kpis/' + kpiId
          : '/api/responsibles/' + responsibleId + '/kpis';

        try {
          const res = await fetch(url, {
            method,
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(buildPayload())
          });

          const savedKpi = await res.json().catch(() => null);

          if (!res.ok) {
            showToast(savedKpi?.error || "Save failed");
            return;
          }

          closeModal();
          await loadKpis(document.getElementById("search").value || "");
          if (isCreateMode && savedKpi?.kpi_id) {
            showToast("KPI created successfully");
            return;
           }
          showToast(kpiId ? "KPI updated successfully" : "KPI created successfully");
        } catch (error) {
          console.error("SAVE KPI ERROR:", error);
          showToast("Save failed: " + error.message);
        }
      }

      async function deleteCurrentKpi() {
        const kpiId = document.getElementById("kpi_id").value;
        if (!kpiId) return;

        const ok = confirm("Delete this KPI?");
        if (!ok) return;

        await deleteKpi(kpiId, true);
      }

      async function deleteKpi(kpiId, fromModal = false) {
        const ok = fromModal ? true : confirm("Delete this KPI?");
        if (!ok) return;

        try {
          const res = await fetch('/api/responsibles/' + responsibleId + '/kpis/' + kpiId, {
            method: "DELETE"
          });

          if (!res.ok) {
            showToast("Delete failed");
            return;
          }

          if (fromModal) closeModal();
          await loadKpis(document.getElementById("search").value || "");
          showToast("KPI deleted");
        } catch (error) {
          showToast("Delete failed");
        }
      }

      function formatParameterDateForInput(value) {
        if (!value) return "";
        const text = String(value).trim();
        return text.length >= 10 ? text.slice(0, 10) : text;
      }

      function resetParameterForm() {
        [
          "parameter_object_id",
          "parameter_kpi_id",
          "parameter_kpi_type",
          "parameter_target_status",
          "parameter_plant_id",
          "parameter_zone_id",
          "parameter_unit_id",
          "parameter_function",
          "parameter_product_line_id",
          "parameter_product_id",
          "parameter_market_id",
          "parameter_customer_id",
          "parameter_target_value",
          "parameter_target_unit",
          "parameter_local_currency",
          "parameter_last_best_target",
          "parameter_previous_target_value",
          "parameter_target_setup_date",
          "parameter_target_start_date",
          "parameter_target_end_date",
          "parameter_set_by_people_id",
          "parameter_approved_by_people_id",
          "parameter_comments"
        ].forEach(id => {
          const el = document.getElementById(id);
          if (el) el.value = "";
        });

        populateAllocationLookupOptions({});

        const setupDateInput = document.getElementById("parameter_target_setup_date");
        if (setupDateInput) {
          setupDateInput.value = new Date().toISOString().slice(0, 10);
        }

        const statusInput = document.getElementById("parameter_target_status");
        if (statusInput) {
          statusInput.value = "Draft";
        }

        const parameterDeleteBtn = document.getElementById("parameterDeleteBtn");
        if (parameterDeleteBtn) {
          parameterDeleteBtn.style.display = "none";
        }
        handleParameterKpiTypeChange();
        updateParameterOverview();
        updateParameterKpiSummary();
      }

      function fillParameterForm(data) {
        const mappings = {
          parameter_object_id: data.kpi_target_allocation_id,
          parameter_kpi_id: data.kpi_id,
          parameter_kpi_type: data.kpi_type,
          parameter_target_status: data.target_status,
          parameter_plant_id: data.plant_id,
          parameter_zone_id: data.zone_id,
          parameter_unit_id: data.unit_id,
          parameter_function: data.function,
          parameter_product_line_id: data.product_line_id,
          parameter_product_id: data.product_id,
          parameter_market_id: data.market_id,
          parameter_customer_id: data.customer_id,
          parameter_target_value: data.target_value ?? "",
          parameter_target_unit: data.target_unit ?? "",
          parameter_local_currency: data.local_currency,
          parameter_last_best_target: data.last_best_target ?? "",
          parameter_previous_target_value: data.previous_target_value ?? "",
          parameter_target_setup_date: formatParameterDateForInput(data.target_setup_date),
          parameter_target_start_date: formatParameterDateForInput(data.target_start_date),
          parameter_target_end_date: formatParameterDateForInput(data.target_end_date),
          parameter_set_by_people_id: data.set_by_people_id,
          parameter_approved_by_people_id: data.approved_by_people_id,
          parameter_comments: data.comments ?? ""
        };

        populateAllocationLookupOptions(mappings);

        Object.keys(mappings).forEach(id => {
          const el = document.getElementById(id);
          if (el) el.value = mappings[id] ?? "";
        });

        handleParameterKpiTypeChange();
        updateParameterOverview();
        updateParameterKpiSummary();
      }

      async function openCreateParameterModal(defaults = {}) {
        await loadKpiNames(defaults.kpi_id || "");
        resetParameterForm();
        const parameterDefaults = {
          parameter_kpi_id: defaults.kpi_id || "",
          parameter_target_value: defaults.target_value ?? "",
          parameter_target_unit: defaults.target_unit || "",
          parameter_set_by_people_id: defaults.set_by_people_id || responsibleId || ""
        };
        populateAllocationLookupOptions(parameterDefaults);
        Object.keys(parameterDefaults).forEach((id) => {
          const el = document.getElementById(id);
          if (el) {
            el.value = parameterDefaults[id] ?? "";
          }
        });
        syncDashboardResponsibleAssignment();
        updateParameterOverview();
        updateParameterKpiSummary();
        document.getElementById("parameterModalTitle").textContent = "Create KPI Target Allocation";
        document.getElementById("parameterModalSubtitle").textContent =
          "Create a KPI target allocation. The responsible defaults to " + (responsibleName || "this person") + ", but you can reassign it before saving.";
        openParameterModal();
      }

      async function openEditParameterModal(parameterObjectId) {
        try {
          const res = await fetch('/api/responsibles/' + responsibleId + '/kpi-objects/' + parameterObjectId);
          const data = await res.json();

          if (!res.ok) {
            throw new Error(data?.error || "Failed to load KPI target allocation");
          }

          await loadKpiNames(data.kpi_id);
          fillParameterForm(data);
          document.getElementById("parameterModalTitle").textContent = "Edit KPI Target Allocation";
          document.getElementById("parameterModalSubtitle").textContent =
            "Update the target allocation details and change the responsible if needed.";
          const parameterDeleteBtn = document.getElementById("parameterDeleteBtn");
          if (parameterDeleteBtn) {
            parameterDeleteBtn.style.display = "inline-flex";
          }
          openParameterModal();
        } catch (error) {
          console.error("OPEN KPI OBJECT MODAL ERROR:", error);
          showToast("Unable to load KPI target allocation: " + error.message);
        }
      }

  function buildParameterPayload() {
   return {
    kpi_id: getParameterFieldValue("parameter_kpi_id"),
    kpi_type: getParameterFieldValue("parameter_kpi_type"),
    target_status: getParameterFieldValue("parameter_target_status"),
    plant_id: getParameterFieldValue("parameter_plant_id"),
    zone_id: getParameterFieldValue("parameter_zone_id"),
    unit_id: getParameterFieldValue("parameter_unit_id"),
    function: getParameterFieldValue("parameter_function"),
    product_line_id: getParameterFieldValue("parameter_product_line_id"),
    product_id: getParameterFieldValue("parameter_product_id"),
    market_id: getParameterFieldValue("parameter_market_id"),
    customer_id: getParameterFieldValue("parameter_customer_id"),
    target_value: getParameterFieldValue("parameter_target_value"),
    target_unit: getParameterFieldValue("parameter_target_unit"),
    local_currency: getParameterFieldValue("parameter_local_currency"),
    last_best_target: getParameterFieldValue("parameter_last_best_target"),
    previous_target_value: getParameterFieldValue("parameter_previous_target_value"),
    target_setup_date: getParameterFieldValue("parameter_target_setup_date"),
    target_start_date: getParameterFieldValue("parameter_target_start_date"),
    target_end_date: getParameterFieldValue("parameter_target_end_date"),
    set_by_people_id: getParameterFieldValue("parameter_set_by_people_id"),
    approved_by_people_id: getParameterFieldValue("parameter_approved_by_people_id"),
    comments: getParameterFieldValue("parameter_comments")
  };
  }

   async function saveParameterKpi() {
  const parameterObjectId = getParameterFieldValue("parameter_object_id");
  const method = parameterObjectId ? "PUT" : "POST";

  const url = parameterObjectId
    ? '/api/responsibles/' + responsibleId + '/kpi-objects/' + parameterObjectId
    : '/api/responsibles/' + responsibleId + '/kpi-objects';

  try {
    const res = await fetch(url, {
      method,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildParameterPayload())
    });

    if (!res.ok) {
      const errorData = await res.json().catch(() => null);
      console.error("SAVE KPI OBJECT API ERROR:", errorData);
      showToast(errorData?.error || "Failed to save KPI target allocation");
      return;
    }

    closeParameterModal();
    await loadParameterKpis();
    showToast(parameterObjectId ? "KPI target allocation updated" : "KPI target allocation created");
  } catch (error) {
    console.error("SAVE KPI OBJECT ERROR:", error);
    showToast("Failed to save: " + error.message);
  }
}

      async function deleteCurrentParameterKpi() {
        const parameterObjectId = getParameterFieldValue("parameter_object_id");
        if (!parameterObjectId) return;

        const ok = confirm("Delete this KPI target allocation?");
        if (!ok) return;

        await deleteParameterKpi(parameterObjectId, true);
      }

      async function deleteParameterKpi(parameterObjectId, fromModal = false) {
        const ok = fromModal ? true : confirm("Delete this KPI target allocation?");
        if (!ok) return;

        try {
          const res = await fetch('/api/responsibles/' + responsibleId + '/kpi-objects/' + parameterObjectId, {
            method: "DELETE"
          });

          if (!res.ok) {
            const errorData = await res.json().catch(() => null);
            showToast(errorData?.error || "Delete failed");
            return;
          }

          if (fromModal) closeParameterModal();
          await loadParameterKpis();
          showToast("KPI target allocation deleted");
        } catch (error) {
          showToast("Delete failed");
        }
      }

      document.getElementById("search").addEventListener("input", (e) => {
        loadKpis(e.target.value);
      });

      document.getElementById("modalBackdrop").addEventListener("click", (e) => {
        if (e.target.id === "modalBackdrop") closeModal();
      });

      document.getElementById("parameterModalBackdrop").addEventListener("click", (e) => {
        if (e.target.id === "parameterModalBackdrop") closeParameterModal();
      });

      [
        "parameter_kpi_id",
        "parameter_kpi_type",
        "parameter_target_status",
        "parameter_plant_id",
        "parameter_zone_id",
        "parameter_unit_id",
        "parameter_function",
        "parameter_product_line_id",
        "parameter_product_id",
        "parameter_market_id",
        "parameter_customer_id",
        "parameter_target_value",
        "parameter_target_unit",
        "parameter_local_currency",
        "parameter_last_best_target",
        "parameter_previous_target_value",
        "parameter_target_setup_date",
        "parameter_target_start_date",
        "parameter_target_end_date",
        "parameter_set_by_people_id",
        "parameter_approved_by_people_id",
        "parameter_comments"
      ].forEach(id => {
        const el = document.getElementById(id);
        if (el) {
          el.addEventListener("input", updateParameterOverview);
          el.addEventListener("change", updateParameterOverview);
        }
      });

      const parameterKpiSelect = document.getElementById("parameter_kpi_id");
      if (parameterKpiSelect) {
        parameterKpiSelect.addEventListener("change", updateParameterKpiSummary);
      }

    const parameterKpiTypeSelect = document.getElementById("parameter_kpi_type");
     if (parameterKpiTypeSelect) {
     parameterKpiTypeSelect.addEventListener("change", () => {
     handleParameterKpiTypeChange();
    autoFillSetByFromUnit();
    });
    }

    const parameterPlantSelect = document.getElementById("parameter_plant_id");
     if (parameterPlantSelect) {
    parameterPlantSelect.addEventListener("change", autoFillSetByFromUnit);
    }
       
      

      window.addEventListener("resize", () => {
      requestAnimationFrame(renderSubjectTree);
     });

     bindOverviewListeners();
     bindHierarchyListeners();
     bindLimitListeners();
     populateRoleOptions("");
     populateAllocationLookupOptions({});
     resetParameterForm();
     updateSubjectTreeCurrentPath();

     (async () => {
       await loadKpis();
       await loadParameterKpis();

       const currentUrl = new URL(window.location.href);
       const editKpiId = currentUrl.searchParams.get("editKpi");

       if (editKpiId) {
         await openEditModal(editKpiId);
         currentUrl.searchParams.delete("editKpi");
         window.history.replaceState({}, "", currentUrl.pathname + currentUrl.search);
       }
     })();
    </script>
  </body>
  </html>
  `);
});



app.get("/responsible/:responsibleId/kpis/:kpiId/edit", async (req, res) => {
  const { responsibleId, kpiId } = req.params;
  res.redirect(`/responsible/${encodeURIComponent(responsibleId)}/dashboard?editKpi=${encodeURIComponent(kpiId)}`);
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

const isPercentageUnit = (unit) => {
  const normalizedUnit = String(unit ?? '').trim().toLowerCase();
  return normalizedUnit === '%' || normalizedUnit === 'percent' || normalizedUnit === 'percentage' || normalizedUnit === 'pct';
};

const parseMetricNumber = (value) => {
  if (value === null || value === undefined || value === '' || value === 'None') {
    return null;
  }
  const parsed = parseFloat(typeof value === 'string' ? value.trim().replace(',', '.') : value);
  return Number.isFinite(parsed) ? parsed : null;
};

const normalizeMetricNumberByUnit = (value, unit) => {
  const parsed = parseMetricNumber(value);
  if (parsed === null) return null;
  if (!isPercentageUnit(unit)) return parsed;
  return Math.abs(parsed) < 1 ? Number((parsed * 100).toFixed(4)) : parsed;
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

const getReadableThresholdLineValues = (lowLimit, target, highLimit) => {
  const low = parseMetricNumber(lowLimit);
  const targetValue = parseMetricNumber(target);
  const high = parseMetricNumber(highLimit);

  const entries = [
    low !== null ? { key: "low", value: low, priority: 0 } : null,
    targetValue !== null ? { key: "target", value: targetValue, priority: 1 } : null,
    high !== null ? { key: "high", value: high, priority: 2 } : null
  ].filter(Boolean);

  if (entries.length < 2) {
    return {
      low,
      target: targetValue,
      high,
      displayLow: low,
      displayTarget: targetValue,
      displayHigh: high,
      hasTightCluster: false,
      actualBand: 0,
      desiredGap: 0
    };
  }

  const sortedEntries = [...entries].sort((a, b) =>
    a.value === b.value ? a.priority - b.priority : a.value - b.value
  );

  const actualBand =
    sortedEntries[sortedEntries.length - 1].value - sortedEntries[0].value;
  const magnitude = Math.max(...sortedEntries.map((entry) => Math.abs(entry.value)), 1);
  const desiredGap = Number(
    Math.min(Math.max(0.25, magnitude * 0.04), 6).toFixed(4)
  );
  const minGap = sortedEntries.slice(1).reduce((currentMin, entry, index) => {
    const previous = sortedEntries[index];
    return Math.min(currentMin, entry.value - previous.value);
  }, Number.POSITIVE_INFINITY);

  const hasTightCluster =
    minGap < desiredGap ||
    actualBand < desiredGap * (sortedEntries.length - 1);

  if (!hasTightCluster) {
    return {
      low,
      target: targetValue,
      high,
      displayLow: low,
      displayTarget: targetValue,
      displayHigh: high,
      hasTightCluster: false,
      actualBand,
      desiredGap
    };
  }

  let displaySorted;
  const targetIndex = sortedEntries.findIndex((entry) => entry.key === "target");

  if (targetIndex >= 0) {
    const anchor = sortedEntries[targetIndex].value;
    displaySorted = sortedEntries.map((entry, index) =>
      Number((anchor + ((index - targetIndex) * desiredGap)).toFixed(4))
    );
  } else {
    const center =
      (sortedEntries[0].value + sortedEntries[sortedEntries.length - 1].value) / 2;
    const start = center - (((sortedEntries.length - 1) * desiredGap) / 2);
    displaySorted = sortedEntries.map((entry, index) =>
      Number((start + (index * desiredGap)).toFixed(4))
    );
  }

  const displayMap = {};
  sortedEntries.forEach((entry, index) => {
    displayMap[entry.key] = displaySorted[index];
  });

  return {
    low,
    target: targetValue,
    high,
    displayLow: displayMap.low ?? low,
    displayTarget: displayMap.target ?? targetValue,
    displayHigh: displayMap.high ?? high,
    hasTightCluster: true,
    actualBand,
    desiredGap
  };
};

const getAutoChartAxisRange = (
  values = [],
  lowLimit = null,
  target = null,
  highLimit = null,
  marginRatio = 0.2
) => {
  const numericValues = [];
  const pushIfNumeric = (value) => {
    const parsed = parseMetricNumber(value);
    if (parsed !== null) {
      numericValues.push(parsed);
    }
  };

  (Array.isArray(values) ? values : [values]).forEach(pushIfNumeric);
  pushIfNumeric(lowLimit);
  pushIfNumeric(target);
  pushIfNumeric(highLimit);

  if (!numericValues.length) {
    return { min: 0, max: 100, sourceMin: 0, sourceMax: 100 };
  }

  const safeMargin = Number.isFinite(marginRatio) && marginRatio >= 0
    ? marginRatio
    : 0.2;
  const sourceMin = Math.min(...numericValues);
  const sourceMax = Math.max(...numericValues);

  let min = sourceMin > 0
    ? sourceMin * (1 - safeMargin)
    : sourceMin < 0
      ? sourceMin * (1 + safeMargin)
      : 0;

  let max = sourceMax > 0
    ? sourceMax * (1 + safeMargin)
    : sourceMax < 0
      ? sourceMax * (1 - safeMargin)
      : 0;

  if (min === max) {
    const pad = Math.max(Math.abs(sourceMax || sourceMin || 1) * safeMargin, 1);
    min = sourceMin - pad;
    max = sourceMax + pad;
  }

  return {
    min: Number(min.toFixed(6)),
    max: Number(max.toFixed(6)),
    sourceMin,
    sourceMax
  };
};

const inferKpiDirection = (kpi = {}) => {
  const explicitDirection = normalizeKpiDirection(
    kpi.target_direction ||
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

const getKpiGoodDirectionMeta = (direction) => {
  const resolvedDirection = normalizeKpiDirection(direction) || 'up';

  if (resolvedDirection === 'down') {
    return {
      value: 'down',
      label: 'Down',
      summary: 'Lower is better',
      examples: 'Scrap, accidents, customer claims',
      accent: '#dc2626',
      background: '#fff5f5',
      border: '#fecaca',
      icon: '&darr;'
    };
  }

  return {
    value: 'up',
    label: 'Up',
    summary: 'Higher is better',
    examples: 'Sales, OTD',
    accent: '#16a34a',
    background: '#f0fdf4',
    border: '#bbf7d0',
    icon: '&uarr;'
  };
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
      console.log(`ðŸ”’ Instance ${instanceId} acquired lock ${lockId}`);
      return { acquired: true, instanceId, lockHash };
    } else {
      return { acquired: false, instanceId, lockHash };
    }
  } catch (error) {
    return { acquired: false, instanceId, error: error.message };
  }
};

const releaseJobLock = async (lockId, lockHashOrInstanceId, maybeLockHash) => {
  const resolvedLockHash = maybeLockHash ?? lockHashOrInstanceId;
  try {
    if (resolvedLockHash) {
      await pool.query('SELECT pg_advisory_unlock($1)', [resolvedLockHash]);
    }
  } catch (error) {
    console.error(`âš ï¸ Could not release lock ${lockId}:`, error.message);
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
  return new OpenAI({
    apiKey: process.env.SECRET_KEY,
  });
};

const formatInputDate = (dateValue) => {
  if (!dateValue) return "";
  const d = new Date(dateValue);
  if (isNaN(d.getTime())) return "";
  return d.toISOString().split("T")[0];
};

const formatDisplayDate = (
  dateValue,
  locale = "en-GB",
  options = { day: "2-digit", month: "short", year: "numeric" }
) => {
  if (!dateValue) return "N/A";
  const d = new Date(dateValue);
  if (isNaN(d.getTime())) return "N/A";
  return d.toLocaleDateString(locale, options);
};

const normalizeText = (value) => {
  const text = String(value ?? "").trim();
  return text ? text : null;
};

const CORRECTIVE_ACTION_STATUS_OPTIONS = [
  "Open",
  "Waiting for validation",
  "Completed",
  "Closed"
];

const OPEN_CORRECTIVE_ACTION_STATUS = "Open";
const CLOSED_CORRECTIVE_ACTION_STATUS = "Closed";

const CORRECTIVE_ACTION_ESCALATION_STAGES = [
  {
    key: "due_day_3_responsible_reminder",
    minOverdueDays: 3,
    audience: "responsible",
    label: "First reminder",
    intro:
      "This corrective action is now 3 days overdue. Please review it and update the status as soon as possible."
  },
  {
    key: "due_day_4_responsible_reminder",
    minOverdueDays: 4,
    audience: "responsible",
    label: "Second reminder",
    intro:
      "This is a second reminder because the corrective action is still open 4 days after the due date."
  },
  {
    key: "due_day_5_responsible_reminder",
    minOverdueDays: 5,
    audience: "responsible",
    label: "Third reminder",
    intro:
      "This is a third reminder because the corrective action is still open 5 days after the due date."
  },
  {
    key: "due_day_6_plant_manager_escalation",
    minOverdueDays: 6,
    audience: "manager",
    label: "Plant manager escalation",
    intro:
      "This corrective action is still open 6 days after the due date and has now been escalated to the plant manager."
  }
];

const normalizeCorrectiveActionStatus = (value, fallback = null) => {
  const text = normalizeText(value);
  if (!text) return fallback;

  const matchedStatus = CORRECTIVE_ACTION_STATUS_OPTIONS.find(
    (option) => option.toLowerCase() === text.toLowerCase()
  );

  return matchedStatus || fallback;
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
    const timeDiff = getCorrectiveActionSortTime(b) - getCorrectiveActionSortTime(a);
    if (timeDiff !== 0) return timeDiff;

    const parsedIdA = parseInt(a.corrective_action_id ?? a.correctiveActionId ?? a.id ?? 0, 10);
    const parsedIdB = parseInt(b.corrective_action_id ?? b.correctiveActionId ?? b.id ?? 0, 10);
    const idA = Number.isFinite(parsedIdA) ? parsedIdA : 0;
    const idB = Number.isFinite(parsedIdB) ? parsedIdB : 0;
    return idB - idA;
  });

const getLatestCorrectiveAction = (actions = []) => {
  const sorted = sortCorrectiveActions(actions);
  return sorted.length ? sorted[0] : null;
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

let correctiveActionEscalationSchemaPromise = null;

const ensureCorrectiveActionEscalationSchema = async () => {
  if (!correctiveActionEscalationSchemaPromise) {
    correctiveActionEscalationSchemaPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS public.corrective_action_escalation_log (
          escalation_id BIGSERIAL PRIMARY KEY,
          corrective_action_id BIGINT NOT NULL,
          escalation_stage TEXT NOT NULL,
          sent_to_email TEXT,
          sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT corrective_action_escalation_log_unique
            UNIQUE (corrective_action_id, escalation_stage)
        )
      `);

      await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_corrective_action_escalation_log_action
        ON public.corrective_action_escalation_log (corrective_action_id)
      `);
    })().catch((error) => {
      correctiveActionEscalationSchemaPromise = null;
      throw error;
    });
  }

  return correctiveActionEscalationSchemaPromise;
};

const getCorrectiveActionOverdueDays = (dateValue, now = new Date()) => {
  if (!dateValue) return 0;

  const dueDate = new Date(dateValue);
  if (isNaN(dueDate.getTime())) return 0;

  const currentDay = new Date(now);
  currentDay.setHours(0, 0, 0, 0);

  const dueDay = new Date(dueDate);
  dueDay.setHours(0, 0, 0, 0);

  const diffMs = currentDay.getTime() - dueDay.getTime();
  if (!Number.isFinite(diffMs) || diffMs < 0) return 0;

  return Math.floor(diffMs / 86400000);
};

const buildCorrectiveActionEscalationEmailHtml = ({ action, stage, overdueDays }) => {
  const actionTitle = [action.kpi_subject, action.indicator_sub_title]
    .filter(Boolean)
    .join(" - ");
  const responsibleName = action.responsible_name || "Responsible owner";
  const managerName = action.manager || "Plant manager";
  const createdOn = formatDisplayDate(action.created_date);
  const dueOn = action.due_date ? formatDisplayDate(action.due_date) : "Not set";
  const isManagerAudience = stage.audience === "manager";
  const theme = isManagerAudience
    ? {
      heroBase: "#7f1d1d",
      heroStart: "#991b1b",
      heroEnd: "#dc2626",
      soft: "#fff1f2",
      softBorder: "#fecaca",
      muted: "#7f1d1d",
      statSurface: "#fff7f7",
      shadow: "rgba(220,38,38,0.14)"
    }
    : {
      heroBase: "#1d4ed8",
      heroStart: "#1d4ed8",
      heroEnd: "#0f766e",
      soft: "#eff6ff",
      softBorder: "#bfdbfe",
      muted: "#1e3a8a",
      statSurface: "#f8fbff",
      shadow: "rgba(37,99,235,0.14)"
    };
  const recipientIntro =
    isManagerAudience
      ? `${escapeHtml(responsibleName)} still has an open corrective action that is overdue by ${overdueDays} day${overdueDays === 1 ? "" : "s"}.`
      : `Your corrective action is overdue by ${overdueDays} day${overdueDays === 1 ? "" : "s"} after the due date.`;
  const statusValue = action.status || OPEN_CORRECTIVE_ACTION_STATUS;
  const normalizedStatus = statusValue.toLowerCase();
  const statusTheme =
    normalizedStatus === "completed" || normalizedStatus === "closed"
      ? {
        bg: "#ecfdf5",
        border: "#a7f3d0",
        color: "#047857"
      }
      : normalizedStatus === "waiting for validation"
        ? {
          bg: "#fff7ed",
          border: "#fed7aa",
          color: "#c2410c"
        }
        : {
          bg: "#fef2f2",
          border: "#fecaca",
          color: "#b91c1c"
        };
  const rootCauseBlock = normalizeText(action.root_cause)
    ? `
      <div style="margin-top:18px;padding:22px 24px;background:#ffffff;border:1px solid #e2e8f0;border-radius:20px;box-shadow:0 10px 24px rgba(15,23,42,0.05);">
        <div style="font-size:12px;font-weight:800;letter-spacing:0.10em;text-transform:uppercase;color:#64748b;margin-bottom:10px;">Root Cause</div>
        <div style="font-size:15px;line-height:1.8;color:#1e293b;">${escapeHtml(action.root_cause)}</div>
      </div>`
    : "";
  const solutionBlock = normalizeText(action.implemented_solution)
    ? `
      <div style="margin-top:18px;padding:22px 24px;background:${theme.statSurface};border:1px solid ${theme.softBorder};border-radius:20px;box-shadow:0 10px 24px ${theme.shadow};">
        <div style="font-size:12px;font-weight:800;letter-spacing:0.10em;text-transform:uppercase;color:${theme.muted};margin-bottom:10px;">Corrective Action</div>
        <div style="font-size:15px;line-height:1.8;color:#1e293b;">${escapeHtml(action.implemented_solution)}</div>
      </div>`
    : "";

  return `<!DOCTYPE html>
  <html>
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width,initial-scale=1.0" />
      <title>${escapeHtml(stage.label)}</title>
    </head>
    <body style="margin:0;padding:0;background:#e8eef5;font-family:'Segoe UI',Arial,sans-serif;color:#0f172a;">
      <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;background:#e8eef5;">
        <tr>
          <td align="center" style="padding:28px 16px;">
            <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="max-width:760px;width:100%;border-collapse:separate;border-spacing:0;background:#ffffff;border:1px solid #dbe3ee;border-radius:28px;overflow:hidden;box-shadow:0 24px 60px rgba(15,23,42,0.12);">
              <tr>
                <td style="padding:0;background:${theme.heroBase};background-image:linear-gradient(135deg, ${theme.heroStart}, ${theme.heroEnd});">
                  <div style="padding:32px 32px 30px;background:radial-gradient(circle at top right, rgba(255,255,255,0.22), transparent 34%);">
                    <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;">
                      <tr>
                        <td style="vertical-align:top;padding-right:14px;">
                          <div style="display:inline-block;padding:8px 14px;border-radius:999px;background:rgba(255,255,255,0.14);border:1px solid rgba(255,255,255,0.18);font-size:11px;font-weight:800;letter-spacing:0.12em;text-transform:uppercase;color:#ffffff;">
                            ${escapeHtml(stage.label)}
                          </div>
                          <h1 style="margin:18px 0 10px;font-size:31px;line-height:1.18;color:#ffffff;letter-spacing:-0.03em;">
                            ${escapeHtml(actionTitle || "Corrective Action")}
                          </h1>
                          <p style="margin:0;font-size:15px;line-height:1.75;color:rgba(255,255,255,0.90);max-width:470px;">
                            ${isManagerAudience ? `Dear ${escapeHtml(managerName)},` : `Dear ${escapeHtml(responsibleName)},`}
                          </p>
                        </td>
                        <td align="right" style="vertical-align:top;width:180px;">
                          <div style="display:inline-block;min-width:150px;padding:16px 18px;border-radius:20px;background:rgba(255,255,255,0.16);border:1px solid rgba(255,255,255,0.18);box-shadow:inset 0 1px 0 rgba(255,255,255,0.18);text-align:left;">
                            <div style="font-size:11px;font-weight:800;letter-spacing:0.10em;text-transform:uppercase;color:rgba(255,255,255,0.74);margin-bottom:6px;">Overdue</div>
                            <div style="font-size:30px;font-weight:800;line-height:1;color:#ffffff;">${overdueDays}</div>
                            <div style="margin-top:6px;font-size:13px;line-height:1.4;color:rgba(255,255,255,0.84);">day${overdueDays === 1 ? "" : "s"} past due</div>
                          </div>
                        </td>
                      </tr>
                    </table>
                  </div>
                </td>
              </tr>

              <tr>
                <td style="padding:28px 32px 0;">
                  <div style="padding:20px 22px;background:${theme.soft};border:1px solid ${theme.softBorder};border-radius:22px;box-shadow:0 10px 28px ${theme.shadow};">
                    <div style="font-size:12px;font-weight:800;letter-spacing:0.10em;text-transform:uppercase;color:${theme.muted};margin-bottom:8px;">
                      Attention Required
                    </div>
                    <div style="font-size:17px;font-weight:700;line-height:1.6;color:#0f172a;">
                      ${recipientIntro}
                    </div>
                    <div style="margin-top:8px;font-size:15px;line-height:1.75;color:#334155;">
                      ${escapeHtml(stage.intro)}
                    </div>
                  </div>
                </td>
              </tr>

              <tr>
                <td style="padding:20px 32px 0;">
                  <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:separate;border-spacing:0 12px;">
                    <tr>
                      <td style="width:50%;padding-right:6px;vertical-align:top;">
                        <div style="padding:16px 18px;background:${theme.statSurface};border:1px solid ${theme.softBorder};border-radius:18px;">
                          <div style="font-size:11px;font-weight:800;letter-spacing:0.10em;text-transform:uppercase;color:#64748b;margin-bottom:8px;">Due Date</div>
                          <div style="font-size:19px;font-weight:800;color:#0f172a;">${escapeHtml(dueOn)}</div>
                        </div>
                      </td>
                      <td style="width:50%;padding-left:6px;vertical-align:top;">
                        <div style="padding:16px 18px;background:#ffffff;border:1px solid ${statusTheme.border};border-radius:18px;">
                          <div style="font-size:11px;font-weight:800;letter-spacing:0.10em;text-transform:uppercase;color:#64748b;margin-bottom:8px;">Status</div>
                          <span style="display:inline-block;padding:8px 12px;border-radius:999px;background:${statusTheme.bg};border:1px solid ${statusTheme.border};font-size:13px;font-weight:800;color:${statusTheme.color};">
                            ${escapeHtml(statusValue)}
                          </span>
                        </div>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>

              <tr>
                <td style="padding:16px 32px 0;">
                  <div style="font-size:12px;font-weight:800;letter-spacing:0.10em;text-transform:uppercase;color:#64748b;margin-bottom:12px;">
                    Action Overview
                  </div>
                  <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:separate;border-spacing:0;background:#ffffff;border:1px solid #e2e8f0;border-radius:20px;overflow:hidden;">
                    <tr>
                      <td style="padding:12px 16px;background:#f8fafc;color:#475569;font-weight:700;width:180px;">Plant</td>
                      <td style="padding:12px 16px;color:#0f172a;">${escapeHtml(action.plant_name || "N/A")}</td>
                    </tr>
                    <tr>
                      <td style="padding:12px 16px;background:#f8fafc;color:#475569;font-weight:700;width:180px;border-top:1px solid #e2e8f0;">Department</td>
                      <td style="padding:12px 16px;color:#0f172a;border-top:1px solid #e2e8f0;">${escapeHtml(action.department_name || "N/A")}</td>
                    </tr>
                    <tr>
                      <td style="padding:12px 16px;background:#f8fafc;color:#475569;font-weight:700;width:180px;border-top:1px solid #e2e8f0;">Responsible</td>
                      <td style="padding:12px 16px;color:#0f172a;border-top:1px solid #e2e8f0;">${escapeHtml(responsibleName)}</td>
                    </tr>
                    <tr>
                      <td style="padding:12px 16px;background:#f8fafc;color:#475569;font-weight:700;width:180px;border-top:1px solid #e2e8f0;">Week</td>
                      <td style="padding:12px 16px;color:#0f172a;border-top:1px solid #e2e8f0;">${escapeHtml(action.week || "N/A")}</td>
                    </tr>
                    <tr>
                      <td style="padding:12px 16px;background:#f8fafc;color:#475569;font-weight:700;width:180px;border-top:1px solid #e2e8f0;">Created On</td>
                      <td style="padding:12px 16px;color:#0f172a;border-top:1px solid #e2e8f0;">${escapeHtml(createdOn)}</td>
                    </tr>
                  </table>
                  ${rootCauseBlock}
                  ${solutionBlock}
                </td>
              </tr>

              <tr>
                <td style="padding:22px 32px 32px;">
                  <div style="padding-top:18px;border-top:1px solid #e2e8f0;font-size:13px;line-height:1.8;color:#64748b;">
                    This notification was sent automatically by the AVOCarbon KPI System to help keep corrective actions visible and on track.
                  </div>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </body>
  </html>`;
};

const getPendingCorrectiveActionEscalations = async () => {
  await ensureCorrectiveActionEscalationSchema();

  const actionsRes = await pool.query(
    `
    SELECT
      ca.corrective_action_id,
      ca.responsible_id,
      ca.kpi_id,
      ca.week,
      ca.status,
      ca.root_cause,
      ca.implemented_solution,
      ca.due_date,
      ca.created_date,
      r.name AS responsible_name,
      r.email AS responsible_email,
      p.name AS plant_name,
      p.manager,
      p.manager_email,
      d.name AS department_name,
      k.subject AS kpi_subject,
      k.indicator_sub_title
    FROM public.corrective_actions ca
    JOIN public."Responsible" r
      ON ca.responsible_id = r.responsible_id
    LEFT JOIN public."Plant" p
      ON r.plant_id = p.plant_id
    LEFT JOIN public."Department" d
      ON r.department_id = d.department_id
    JOIN public."Kpi" k
      ON ca.kpi_id = k.kpi_id
    WHERE COALESCE(ca.status, $1::text) = $1::text
      AND ca.due_date IS NOT NULL
      AND ca.due_date::date <= CURRENT_DATE - INTERVAL '3 days'
    ORDER BY ca.due_date ASC, ca.corrective_action_id ASC
    `,
    [OPEN_CORRECTIVE_ACTION_STATUS]
  );

  if (!actionsRes.rows.length) {
    return [];
  }

  const actionIds = actionsRes.rows
    .map((row) => String(row.corrective_action_id ?? "").trim())
    .filter(Boolean);
  const escalationLogRes = actionIds.length
    ? await pool.query(
      `
        SELECT corrective_action_id, escalation_stage
        FROM public.corrective_action_escalation_log
        WHERE corrective_action_id = ANY($1::bigint[])
        `,
      [actionIds]
    )
    : { rows: [] };

  const sentStagesByActionId = escalationLogRes.rows.reduce((acc, row) => {
    const actionId = String(row.corrective_action_id);
    if (!acc[actionId]) {
      acc[actionId] = new Set();
    }
    acc[actionId].add(row.escalation_stage);
    return acc;
  }, {});

  const pendingEscalations = [];

  actionsRes.rows.forEach((action) => {
    const overdueDays = getCorrectiveActionOverdueDays(action.due_date);
    const sentStages = sentStagesByActionId[String(action.corrective_action_id)] || new Set();

    CORRECTIVE_ACTION_ESCALATION_STAGES.forEach((stage) => {
      if (overdueDays < stage.minOverdueDays || sentStages.has(stage.key)) {
        return;
      }

      const recipientEmail = normalizeText(
        stage.audience === "manager" ? action.manager_email : action.responsible_email
      );

      if (!recipientEmail) {
        return;
      }

      pendingEscalations.push({
        action,
        stage,
        overdueDays,
        recipientEmail
      });
    });
  });

  return pendingEscalations;
};

const sendCorrectiveActionEscalationEmail = async ({
  transporter,
  action,
  stage,
  overdueDays,
  recipientEmail
}) => {
  const kpiLabel = [action.kpi_subject, action.indicator_sub_title]
    .filter(Boolean)
    .join(" - ");
  const subject =
    stage.audience === "manager"
      ? `Escalation: Corrective action overdue by ${overdueDays} days - ${kpiLabel || action.corrective_action_id}`
      : `${stage.label}: Corrective action overdue by ${overdueDays} days - ${kpiLabel || action.corrective_action_id}`;

  await transporter.sendMail({
    from: '"AVOCarbon KPI System" <administration.STS@avocarbon.com>',
    to: recipientEmail,
    subject,
    html: buildCorrectiveActionEscalationEmailHtml({ action, stage, overdueDays })
  });

  await pool.query(
    `
    INSERT INTO public.corrective_action_escalation_log
      (corrective_action_id, escalation_stage, sent_to_email)
    VALUES ($1, $2, $3)
    ON CONFLICT (corrective_action_id, escalation_stage) DO NOTHING
    `,
    [action.corrective_action_id, stage.key, recipientEmail]
  );
};

const runCorrectiveActionEscalationJob = async () => {
  const pendingEscalations = await getPendingCorrectiveActionEscalations();

  if (!pendingEscalations.length) {
    console.log("[Corrective Action Escalation] No pending escalation emails.");
    return { sent: 0, pending: 0 };
  }

  const transporter = createTransporter();
  let sentCount = 0;

  for (const escalation of pendingEscalations) {
    try {
      await sendCorrectiveActionEscalationEmail({
        transporter,
        action: escalation.action,
        stage: escalation.stage,
        overdueDays: escalation.overdueDays,
        recipientEmail: escalation.recipientEmail
      });

      sentCount += 1;
      console.log(
        `[Corrective Action Escalation] ${escalation.stage.key} sent for corrective action ${escalation.action.corrective_action_id} to ${escalation.recipientEmail}`
      );
      await new Promise((resolve) => setTimeout(resolve, 500));
    } catch (error) {
      console.error(
        `[Corrective Action Escalation] Failed to send ${escalation.stage.key} for corrective action ${escalation.action.corrective_action_id}:`,
        error.message
      );
    }
  }

  return {
    sent: sentCount,
    pending: pendingEscalations.length
  };
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
  const statusText = normalizeCorrectiveActionStatus(action.status, "Open");
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

const MAX_ASSISTANT_HISTORY_MESSAGES = 14;

const normalizeAssistantConversationHistory = (history = []) =>
  (Array.isArray(history) ? history : [])
    .map((entry) => {
      const role = entry?.role === "assistant" ? "assistant" : entry?.role === "user" ? "user" : null;
      const content = normalizeText(entry?.content ?? entry?.contextText ?? entry?.text ?? entry?.message);
      if (!role || !content) return null;
      return { role, content };
    })
    .filter(Boolean)
    .slice(-MAX_ASSISTANT_HISTORY_MESSAGES);

const buildAssistantConversationMemory = (history = []) => {
  const normalizedHistory = normalizeAssistantConversationHistory(history);
  const recentUserInputs = normalizedHistory
    .filter((entry) => entry.role === "user")
    .map((entry) => entry.content)
    .slice(-6);
  const recentAssistantQuestions = normalizedHistory
    .filter((entry) => entry.role === "assistant")
    .map((entry) => entry.content)
    .filter((content) => content.includes("?") || /\n\s*1\.\s+/m.test(content))
    .slice(-4);

  return {
    history_available: normalizedHistory.length > 0,
    recent_user_inputs: recentUserInputs,
    recent_assistant_questions: recentAssistantQuestions,
    last_user_input: recentUserInputs.length ? recentUserInputs[recentUserInputs.length - 1] : null,
    completed_turns: Math.floor(normalizedHistory.length / 2)
  };
};

const buildKpiScopedKnowledgeBaseQuery = ({
  message,
  conversationHistory,
  selectedKpi,
  selectedKpiSummary,
  selectedKpiDelayFocus
}) => {
  const queryParts = [String(message || "").trim()];
  const recentConversationInputs = normalizeAssistantConversationHistory(conversationHistory)
    .filter((entry) => entry.role === "user")
    .map((entry) => entry.content)
    .slice(-4);

  if (recentConversationInputs.length) {
    queryParts.push(`Recent conversation context: ${recentConversationInputs.join(" | ")}`);
  }

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
  message,
  conversationHistory
}) => {
  const assistantKpis = buildAssistantKpiContext(kpis);
  const normalizedConversationHistory = normalizeAssistantConversationHistory(conversationHistory);
  const conversationMemory = buildAssistantConversationMemory(normalizedConversationHistory);
  const selectedKpi = assistantKpis.find((kpi) =>
    String(kpi.kpi_id) === String(selectedKpiId) ||
    String(kpi.kpi_values_id) === String(selectedKpiId)
  ) || null;
  const selectedKpiSummary = buildSelectedKpiSummary(selectedKpi);
  const selectedKpiDelayFocus = buildSelectedKpiDelayFocus(selectedKpi);
  const knowledgeBaseQuery = buildKpiScopedKnowledgeBaseQuery({
    message,
    conversationHistory: normalizedConversationHistory,
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

  // if (isFastKpiAssistantRequest({
  //   message,
  //   selectedKpi,
  //   knowledgeBaseContext
  // })) {
  //   return fastLocalReply;
  // }

  const promptKpiContext = selectedKpi ? [selectedKpi] : assistantKpis.slice(0, 8);
  const promptKnowledgeMatches = (knowledgeBaseContext.matches || []).slice(0, 3);
  const promptKnowledgeRelated = (knowledgeBaseContext.related || []).slice(0, 2);

  const contextPrompt = `
CONFIRMED PAGE CONTEXT
${JSON.stringify({
    responsible: responsible?.name || null,
    plant: responsible?.plant_name || null,
    department: responsible?.department_name || null,
    week: week || null
  }, null, 2)}

KNOWN CONVERSATION MEMORY
${JSON.stringify(conversationMemory, null, 2)}

RECENT KPI CONTEXT
${JSON.stringify(promptKpiContext, null, 2)}

SELECTED KPI
${selectedKpi ? JSON.stringify(selectedKpi, null, 2) : "None selected"}

SELECTED KPI SUMMARY
${JSON.stringify(selectedKpiSummary, null, 2)}

MATCHED KNOWLEDGE BASE NODES
${JSON.stringify(promptKnowledgeMatches, null, 2)}

RELATED KNOWLEDGE BASE NODES
${JSON.stringify(promptKnowledgeRelated, null, 2)}

USER MESSAGE
${message}

IMPORTANT
- Follow the system prompt exactly.
- Treat the conversation history as the active memory for this diagnosis.
- Use the available memory before asking any follow-up question.
- Do not repeat a question if the answer is already present or strongly implied in the history.
- If the user's latest reply is short, resolve it using the immediately preceding assistant question and options.
- Use conversation data first, then the provided knowledge base nodes, then general knowledge only if really necessary.
- Use confirmed page/KPI context as part of the conversation evidence.
- Use the knowledge base to support diagnosis.
- Do not invent causes, actions, owners, deadlines, metrics, or evidence.
- During diagnosis, prioritize asking one short question over giving explanations.
- If support is insufficient, ask the next best short diagnostic question.
- If a user proposes a shortcut or estimate, evaluate whether it is reasonable before accepting it.
`.trim();

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

    const systemPrompt = `
You are an intelligent assistant designed to capture, structure, analyze, and refine user information through natural conversation in order to identify:

- a root cause
- an action
- an owner
- a reasonable deadline

Your behavior follows these core principles:

### 1. Memory and Context
- You remember all previously provided user inputs.
- You NEVER ask a question if the answer has already been provided.
- You continuously update an internal structured dataset when a schema or JSON target exists.
- This dataset is STRICTLY internal and must NEVER be shown, printed, summarized as a structure, or referenced explicitly.
- You reuse past answers when logically valid.
- You may infer missing links from prior answers, but never invent facts.

### 2. Main Objective
Your goal is to guide the user efficiently toward a usable diagnosis and decision by identifying:
- the real problem
- the root cause
- the practical corrective action
- who should own it
- a realistic deadline

You must keep moving toward these five outputs unless the user changes the topic.

### 3. Reasoning: Cause and Effect
- You analyze relationships between answers.
- You detect cause-effect links across the conversation.
- You distinguish between:
  - symptom
  - cause
  - root cause
  - effect
  - action
  - owner
  - deadline
- You refine future questions based on previous answers.
- If an answer implies another field:
  - populate it internally if confidence is high, or
  - ask for confirmation if uncertainty remains.
- You must not confuse a complaint, an assumption, and a validated root cause.

### 4. Detecting User-Proposed Solutions
- At any step, if the user proposes an action, shortcut, workaround, decision, operating rule, estimate, bypass, or organizational change, treat it as a potential solution.
- You must detect such proposals directly from the user's wording, even if they are implicit.
- Examples of user-proposed solutions include:
  - "we should estimate"
  - "let's not wait for supplier quotes"
  - "sales can launch with assumptions"
  - "engineering should fill the gaps later"
  - "we should add a checklist"
  - "we should block the file"
- When a proposed solution appears, do NOT accept it automatically.
- First evaluate whether it is reasonable.

### 5. Reasonableness Test for Proposed Solutions
Whenever the user suggests a solution, test it using these criteria:

1. Does it reduce the delay or problem in a real way?
2. Does it create a bigger downstream risk?
3. Is it acceptable for quality, profitability, customer credibility, and execution?
4. Is it based on controlled assumptions or on guesswork?
5. Can it be framed with clear conditions of use?
6. Does it require validation, containment, or boundaries?
7. Is it a root-cause solution or just a speed shortcut?

If the solution seems reasonable:
- accept it provisionally
- refine it
- add the conditions that make it safe and professional

If the solution seems only partly reasonable:
- keep the useful part
- challenge the risky part
- reformulate it into a controlled version

If the solution is not reasonable:
- explain briefly why
- redirect toward a safer alternative
- continue the questioning

### 6. Controlled Acceptance of Estimation and Assumptions
If the user proposes estimating costs, using assumptions, or not waiting for missing data:
- do not reject automatically
- assess whether a controlled estimate is acceptable
- distinguish between:
  - professional hypothesis
  - market benchmark
  - should-cost logic
  - historical comparable
  - blind guess
- Controlled estimation may be acceptable only if:
  - the missing data are clearly identified
  - the uncertainty is visible
  - the impact is limited or understood
  - the estimate is temporary
  - someone owns the follow-up
  - the quote can later be corrected or confirmed if needed
- If estimation would expose the business to serious pricing error, margin loss, technical mismatch, or customer credibility damage, you must challenge it.

### 7. Question Strategy
- Ask only one question at a time by default.
- Questions must be clear, direct, and useful.
- Keep them short.
- Prefer questions that help discriminate between causes.
- Prefer structured answer options when useful.

#### Format for guided questions
Always put each option on its own line using this exact format:

Question text:
1. Option A
2. Option B
3. Option C

Example:

Where is the biggest delay?
1. Internal missing data
2. Supplier response
3. Validation
4. Priorities changing

### 8. Smart Grouping
- When useful, you may group 3 to 4 related elements in one question.
- Use grouping especially when:
  - no active deep discussion is underway
  - several fields are missing
  - the user appears comfortable progressing faster
- Keep grouped questions easy to answer.
- Do not overload the user.

Example:

What is mainly happening?
1. Sales launches too early
2. Suppliers are too slow
3. Approvals are too slow
4. Priorities keep changing

### 9. Answer Autocomplete
- If you already have enough information:
  - do not ask the question
  - fill the field internally
- Ask for confirmation only when uncertainty matters.
- Never pretend certainty when evidence is weak.

### 10. Internal Field Handling
If a target schema or JSON structure exists:
- use it internally as the target schema
- progressively fill it
- never invent missing values
- only populate fields when confidence is sufficient
- never display the internal schema
- never expose internal updates

### 11. Tone and Interaction Style
- Always communicate adult-to-adult.
- Be neutral, concise, and respectful.
- Never be condescending.
- Never be overly enthusiastic.
- Do not overwhelm the user with long explanations.
- Let the user think and answer independently.
- Be direct when the user's proposed solution is risky.

### 12. Truth and Reliability
- Never lie.
- Never invent data.
- Never assume facts without enough evidence.
- If unsure:
  - ask, or
  - leave the field unfilled
- If using general knowledge, keep it separate from conversation-derived facts.
- Never validate a root cause without enough support from the conversation.

### 13. Knowledge Priority
Use knowledge in this order:
1. conversation data
2. provided files or knowledge base
3. general knowledge only when necessary

Always distinguish between what the user said and what you infer.

### 14. Response Discipline
- Keep responses short and purposeful.
- Ask or act, not both unless necessary.
- Avoid repeating known information unless needed for precision.
- Do not produce long summaries during the diagnosis phase.
- Move the discussion forward.

### 15. Diagnostic Logic
At each step, you must internally determine whether the user is expressing:
- a symptom
- a suspected cause
- a proposed solution
- a constraint
- a decision
- an owner
- a deadline

Then decide whether to:
- store it
- challenge it
- refine it
- ask the next best question

### 16. Root Cause Discipline
Do not stop at the first plausible cause.

Try to separate:
- what is happening
- why it happens
- why that is allowed to happen
- what system rule, behavior, or gap creates recurrence

A root cause should explain why the problem repeats or survives.

### 17. Action Quality
An action is acceptable only if it is:
- specific
- practical
- owned
- time-bound
- likely to reduce the problem

Do not accept vague actions like:
- "communicate better"
- "be faster"
- "follow up more"

Refine them into operational actions.

### 18. Owner Quality
If the user mentions a team, function, or role that clearly owns the issue, capture it internally.
If ownership is unclear, ask.
Prefer a real accountable owner rather than a vague collective when possible.

### 19. Deadline Quality
If the user proposes a deadline, test whether it is realistic.

A reasonable deadline should fit:
- urgency
- implementation complexity
- required coordination
- business impact

If the deadline is unrealistic, challenge it briefly and ask for a more credible one.

### 20. Execution Loop
At each step:

1. Analyze known data from the conversation and internal dataset.
2. Detect any missing or uncertain fields among:
   - problem
   - root cause
   - action
   - owner
   - deadline
3. Detect whether the user has already proposed a solution.
4. If yes, test whether it is reasonable.
5. Decide whether to:
   - fill automatically if confidence is high
   - ask one targeted question
   - challenge and refine a risky proposed solution
6. Prefer grouping when it clearly improves efficiency.
7. Continue until the diagnosis is good enough or the user stops.

### 21. Expected Interaction Pattern
When the user gives a statement such as:

"Sales are pushing hard without all the data. It is possible to go faster by estimating some cost and making professional hypotheses. At the same time our suppliers are too long to quote so we should just estimate."

You must recognize that the user has already provided:
- symptoms
- possible causes
- at least one proposed solution

You must then test that solution rather than simply accept it.

A good next step would be a question like:

What is the biggest risk if you estimate instead of waiting?
1. Pricing too low
2. Technical mismatch
3. Customer credibility
4. Limited risk if assumptions are controlled

### 22. Final Goal
Efficient, natural, structured diagnosis with:
- minimal friction
- no redundancy
- strong reasoning
- detection of user-proposed solutions
- validation of whether those solutions are reasonable
- zero leakage of internal data structures

### 23. Conversation Continuity
- The provided prior assistant/user messages are the active memory of this discussion.
- Never restart the diagnosis unless the user clearly changes topic.
- Before asking a question, check whether that question was already asked or already answered.
- If the user answers with a short option like "1", "2", or "3", interpret it as the answer to the most recent guided question in the conversation.
- Prefer the next unanswered diagnostic question, not a restatement of the previous one.
`.trim();

    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        {
          role: "system",
          content: systemPrompt
        },
        {
          role: "system",
          content: contextPrompt
        },
        ...normalizedConversationHistory.map((entry) => ({
          role: entry.role,
          content: entry.content
        })),
        {
          role: "user",
          content: message
        }
      ],
      temperature: 0.1,
      max_tokens: 220
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
// getResponsibleWithKPIs â€” now also fetches existing corrective
// action data (root_cause, implemented_solution, evidence)
// ============================================================
const formatIsoDateValue = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 10);
};

const parseWeekLabelDate = (weekLabel) => {
  const text = normalizeOptionalTextInput(weekLabel);
  const match = text ? text.match(/^(\d{4})-Week(\d{1,2})$/i) : null;
  if (!match) return null;

  const year = Number(match[1]);
  const weekNumber = Number(match[2]);
  if (!Number.isInteger(year) || !Number.isInteger(weekNumber) || weekNumber < 1 || weekNumber > 53) {
    return null;
  }

  return new Date(Date.UTC(year, 0, 1 + (weekNumber - 1) * 7));
};

const deriveKpiResultDate = (periodLabel) =>
  normalizeOptionalDateInput(periodLabel) ||
  formatIsoDateValue(parseWeekLabelDate(periodLabel)) ||
  formatIsoDateValue(new Date());

const deriveKpiResultPeriodType = (periodLabel) => {
  const text = normalizeOptionalTextInput(periodLabel);
  if (!text) return "Date";
  if (/^\d{4}-Week\d{1,2}$/i.test(text)) return "Week";
  if (/^\d{4}-\d{2}$/.test(text)) return "Month";
  return "Period";
};

const getKpiResultDisplayPeriod = (row = {}) =>
  normalizeOptionalTextInput(row.week) ||
  normalizeOptionalTextInput(row.period_label) ||
  normalizeOptionalDateInput(row.result_date) ||
  "";

const loadFormResponsibleContext = async (responsibleId) => {
  const normalizedResponsibleId = normalizeOptionalIntegerInput(responsibleId);
  const fallbackName = normalizedResponsibleId
    ? `Responsible #${normalizedResponsibleId}`
    : "KPI Owner";

  if (!normalizedResponsibleId) {
    return {
      responsible_id: null,
      people_id: null,
      name: fallbackName,
      email: null,
      plant_name: "Allocated scope",
      department_name: ""
    };
  }

  try {
    const legacyResponsible = await loadLegacyResponsibleIdentity(normalizedResponsibleId);
    const matchedPeopleFromLegacy = legacyResponsible
      ? await findPeopleContextForLegacyResponsible(legacyResponsible)
      : null;
    const directPeopleContext = await loadPeopleContextByPeopleId(normalizedResponsibleId);
    const resolvedPeopleContext = matchedPeopleFromLegacy || directPeopleContext;

    return {
      responsible_id: normalizedResponsibleId,
      people_id: resolvedPeopleContext?.people_id ?? directPeopleContext?.people_id ?? null,
      name:
        resolvedPeopleContext?.name ||
        legacyResponsible?.name ||
        directPeopleContext?.name ||
        fallbackName,
      email:
        resolvedPeopleContext?.email ||
        legacyResponsible?.email ||
        directPeopleContext?.email ||
        null,
      plant_name:
        resolvedPeopleContext?.plant_name ||
        directPeopleContext?.plant_name ||
        "Allocated scope",
      department_name:
        resolvedPeopleContext?.department_name ||
        directPeopleContext?.department_name ||
        ""
    };
  } catch (error) {
    return {
      responsible_id: normalizedResponsibleId,
      people_id: normalizedResponsibleId,
      name: fallbackName,
      email: null,
      plant_name: "Allocated scope",
      department_name: ""
    };
  }
};

const resolveRecordedByPeopleId = async (candidateId) => {
  const peopleId = normalizeOptionalIntegerInput(candidateId);
  if (!peopleId) return null;

  const result = await pool.query(
    `SELECT people_id FROM public.people WHERE people_id = $1 LIMIT 1`,
    [peopleId]
  );

  return result.rows[0]?.people_id ?? null;
};

const optionalTableAvailabilityCache = new Map();

const hasPublicTable = async (tableName) => {
  const normalizedName = normalizeOptionalTextInput(tableName);
  if (!normalizedName) return false;

  if (optionalTableAvailabilityCache.has(normalizedName)) {
    return optionalTableAvailabilityCache.get(normalizedName);
  }

  const result = await pool.query(
    `SELECT to_regclass($1) AS regclass`,
    [`public.${normalizedName}`]
  );

  const exists = Boolean(result.rows[0]?.regclass);
  optionalTableAvailabilityCache.set(normalizedName, exists);
  return exists;
};

const actionPlanTableAvailabilityCache = new Map();
const ACTION_PLAN_SUBJECT_CODE_PREFIX = "kpi-form";

const hasActionPlanTable = async (tableName) => {
  const normalizedName = normalizeOptionalTextInput(tableName);
  if (!normalizedName) return false;

  if (actionPlanTableAvailabilityCache.has(normalizedName)) {
    return actionPlanTableAvailabilityCache.get(normalizedName);
  }

  try {
    const result = await actionPlanPool.query(
      `SELECT to_regclass($1) AS regclass`,
      [`public.${normalizedName}`]
    );

    const exists = Boolean(result.rows[0]?.regclass);
    actionPlanTableAvailabilityCache.set(normalizedName, exists);
    return exists;
  } catch (error) {
    console.error("Action Plan table availability check failed:", error.message);
    actionPlanTableAvailabilityCache.set(normalizedName, false);
    return false;
  }
};

const canUseActionPlanCorrectiveActions = async () => {
  const [hasActionTable, hasSujetTable] = await Promise.all([
    hasActionPlanTable("action"),
    hasActionPlanTable("sujet")
  ]);

  return hasActionTable && hasSujetTable;
};

const encodeActionPlanPeriodToken = (value) => {
  const normalizedValue = normalizeOptionalTextInput(value) || "period";
  return Buffer.from(normalizedValue, "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
};

const decodeActionPlanPeriodToken = (value) => {
  const token = normalizeOptionalTextInput(value);
  if (!token) return null;

  const base64 = token.replace(/-/g, "+").replace(/_/g, "/");
  const padding = base64.length % 4 === 0
    ? ""
    : "=".repeat(4 - (base64.length % 4));

  try {
    const decoded = Buffer.from(base64 + padding, "base64").toString("utf8");
    return normalizeOptionalTextInput(decoded);
  } catch (error) {
    return null;
  }
};

const buildActionPlanSubjectCode = ({
  kpiTargetAllocationId,
  kpiId,
  periodLabel
}) => {
  const allocationId = normalizeOptionalIntegerInput(kpiTargetAllocationId);
  const normalizedKpiId = normalizeOptionalIntegerInput(kpiId);
  const normalizedPeriodLabel =
    normalizeOptionalTextInput(periodLabel) ||
    formatIsoDateValue(new Date()) ||
    "period";

  if (!allocationId || !normalizedKpiId) return null;

  return [
    ACTION_PLAN_SUBJECT_CODE_PREFIX,
    allocationId,
    normalizedKpiId,
    encodeActionPlanPeriodToken(normalizedPeriodLabel)
  ].join("|");
};

const buildActionPlanSubjectPattern = ({
  kpiTargetAllocationId,
  kpiId
}) => {
  const allocationId = normalizeOptionalIntegerInput(kpiTargetAllocationId);
  const normalizedKpiId = normalizeOptionalIntegerInput(kpiId);
  if (!allocationId || !normalizedKpiId) return null;
  return `${ACTION_PLAN_SUBJECT_CODE_PREFIX}|${allocationId}|${normalizedKpiId}|%`;
};

const parseActionPlanSubjectCode = (value) => {
  const code = normalizeOptionalTextInput(value);
  if (!code) return null;

  const [prefix, rawAllocationId, rawKpiId, rawPeriodToken] = code.split("|");
  if (
    prefix !== ACTION_PLAN_SUBJECT_CODE_PREFIX ||
    rawAllocationId === undefined ||
    rawKpiId === undefined ||
    rawPeriodToken === undefined
  ) {
    return null;
  }

  const allocationId = normalizeOptionalIntegerInput(rawAllocationId);
  const kpiId = normalizeOptionalIntegerInput(rawKpiId);
  const periodLabel = decodeActionPlanPeriodToken(rawPeriodToken);

  if (!allocationId || !kpiId || !periodLabel) {
    return null;
  }

  return {
    kpiTargetAllocationId: allocationId,
    kpiId,
    periodLabel
  };
};

const buildActionPlanSubjectMetadata = ({
  kpiTargetAllocationId,
  kpiId,
  periodLabel,
  subject,
  kpiName
}) => {
  const label =
    normalizeOptionalTextInput(kpiName) ||
    normalizeOptionalTextInput(subject) ||
    `KPI ${kpiId}`;
  const periodText =
    normalizeOptionalTextInput(periodLabel) ||
    formatIsoDateValue(new Date()) ||
    "current period";

  return {
    title: `${label} - ${periodText}`,
    description:
      `Corrective actions captured from the KPI form for "${label}" ` +
      `during ${periodText}. Allocation ${kpiTargetAllocationId}, KPI ${kpiId}.`
  };
};

const resolveActionPlanResponsibleEmail = ({
  responsibleName,
  responsibleContext
}) => {
  const normalizedResponsibleName = normalizeOptionalTextInput(responsibleName);
  const normalizedContextName = normalizeOptionalTextInput(responsibleContext?.name);
  const normalizedContextEmail = normalizeOptionalTextInput(responsibleContext?.email);

  if (normalizedResponsibleName && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalizedResponsibleName)) {
    return normalizedResponsibleName;
  }

  if (
    normalizedContextEmail &&
    (!normalizedResponsibleName ||
      !normalizedContextName ||
      normalizedResponsibleName.toLowerCase() === normalizedContextName.toLowerCase())
  ) {
    return normalizedContextEmail;
  }

  return null;
};

const mapActionPlanActionRow = (row = {}) => {
  const subjectInfo = parseActionPlanSubjectCode(row.sujet_code);
  if (!subjectInfo) return null;

  const status =
    normalizeCorrectiveActionStatus(row.status, OPEN_CORRECTIVE_ACTION_STATUS) ||
    OPEN_CORRECTIVE_ACTION_STATUS;

  return {
    id: row.id,
    corrective_action_id: row.id,
    kpi_values_id: subjectInfo.kpiTargetAllocationId,
    kpi_target_allocation_id: subjectInfo.kpiTargetAllocationId,
    kpi_id: subjectInfo.kpiId,
    week: subjectInfo.periodLabel,
    period_label: subjectInfo.periodLabel,
    root_cause: normalizeOptionalTextInput(row.titre) || "",
    implemented_solution: normalizeOptionalTextInput(row.description) || "",
    status,
    due_date: formatIsoDateValue(row.due_date),
    responsible: normalizeOptionalTextInput(row.responsable) || "",
    email_responsable: normalizeOptionalTextInput(row.email_responsable),
    created_date: row.created_at,
    updated_date: row.updated_at,
    closed_date: row.closed_date,
    sujet_id: row.sujet_id,
    sujet_code: normalizeOptionalTextInput(row.sujet_code),
    sujet_title: normalizeOptionalTextInput(row.sujet_title)
  };
};

const loadActionPlanCorrectiveActionMaps = async (kpiRows = [], currentPeriodLabel = null) => {
  if (!(await canUseActionPlanCorrectiveActions())) {
    return {
      currentByAllocationId: {},
      historyByAllocationId: {}
    };
  }

  const patterns = Array.from(new Set(
    (Array.isArray(kpiRows) ? kpiRows : [])
      .map((row) => buildActionPlanSubjectPattern({
        kpiTargetAllocationId: row.kpi_target_allocation_id ?? row.kpi_values_id,
        kpiId: row.kpi_id
      }))
      .filter(Boolean)
  ));

  if (!patterns.length) {
    return {
      currentByAllocationId: {},
      historyByAllocationId: {}
    };
  }

  try {
    const actionsRes = await actionPlanPool.query(
      `
      SELECT
        a.id,
        a.sujet_id,
        a.type,
        a.titre,
        a.description,
        a.status,
        a.responsable,
        a.email_responsable,
        a.due_date,
        a.created_at,
        a.updated_at,
        a.closed_date,
        s.code AS sujet_code,
        s.titre AS sujet_title
      FROM public.action a
      JOIN public.sujet s
        ON s.id = a.sujet_id
      WHERE s.code LIKE ANY($1::text[])
        AND LOWER(COALESCE(a.type, 'action')) = 'action'
      ORDER BY COALESCE(a.updated_at, a.created_at) DESC, a.id DESC
      `,
      [patterns]
    );

    const normalizedCurrentPeriod = normalizeOptionalTextInput(currentPeriodLabel);
    const currentByAllocationId = {};
    const historyByAllocationId = {};

    actionsRes.rows.forEach((row) => {
      const mappedAction = mapActionPlanActionRow(row);
      if (!mappedAction) return;

      const allocationKey = String(
        mappedAction.kpi_target_allocation_id ?? mappedAction.kpi_values_id ?? ""
      ).trim();
      if (!allocationKey) return;

      if (!historyByAllocationId[allocationKey]) {
        historyByAllocationId[allocationKey] = [];
      }
      historyByAllocationId[allocationKey].push(mappedAction);

      if (normalizedCurrentPeriod && mappedAction.period_label === normalizedCurrentPeriod) {
        if (!currentByAllocationId[allocationKey]) {
          currentByAllocationId[allocationKey] = [];
        }
        currentByAllocationId[allocationKey].push(mappedAction);
      }
    });

    return {
      currentByAllocationId,
      historyByAllocationId
    };
  } catch (error) {
    console.error("Failed to load Action Plan corrective actions:", error.message);
    return {
      currentByAllocationId: {},
      historyByAllocationId: {}
    };
  }
};

const ensureActionPlanSubject = async ({
  kpiTargetAllocationId,
  kpiId,
  periodLabel,
  subject,
  kpiName
}) => {
  const subjectCode = buildActionPlanSubjectCode({
    kpiTargetAllocationId,
    kpiId,
    periodLabel
  });
  if (!subjectCode) return null;

  const metadata = buildActionPlanSubjectMetadata({
    kpiTargetAllocationId,
    kpiId,
    periodLabel,
    subject,
    kpiName
  });

  const existingSubjectRes = await actionPlanPool.query(
    `SELECT id FROM public.sujet WHERE code = $1 LIMIT 1`,
    [subjectCode]
  );

  const existingSubjectId = normalizeOptionalIntegerInput(existingSubjectRes.rows[0]?.id);
  if (existingSubjectId) {
    await actionPlanPool.query(
      `
      UPDATE public.sujet
      SET
        titre = $2,
        description = $3,
        updated_at = NOW(),
        inserted_by = COALESCE(inserted_by, $4)
      WHERE id = $1
      `,
      [existingSubjectId, metadata.title, metadata.description, "KPI form"]
    );

    return {
      sujetId: existingSubjectId,
      subjectCode
    };
  }

  const insertedSubjectRes = await actionPlanPool.query(
    `
    INSERT INTO public.sujet (code, titre, description, inserted_by)
    VALUES ($1, $2, $3, $4)
    RETURNING id
    `,
    [subjectCode, metadata.title, metadata.description, "KPI form"]
  );

  return {
    sujetId: normalizeOptionalIntegerInput(insertedSubjectRes.rows[0]?.id),
    subjectCode
  };
};

const syncActionPlanCorrectiveActionsForPeriod = async ({
  kpiTargetAllocationId,
  kpiId,
  periodLabel,
  subject,
  kpiName,
  actions = [],
  responsibleContext = null
}) => {
  const subjectCode = buildActionPlanSubjectCode({
    kpiTargetAllocationId,
    kpiId,
    periodLabel
  });

  if (!subjectCode || !(await canUseActionPlanCorrectiveActions())) {
    return { savedCount: 0, deletedCount: 0 };
  }

  const submittedActions = Array.isArray(actions) ? actions : [];
  const meaningfulActions = submittedActions.filter(hasMeaningfulCorrectiveActionInput);

  const existingSubjectRes = await actionPlanPool.query(
    `SELECT id FROM public.sujet WHERE code = $1 LIMIT 1`,
    [subjectCode]
  );

  let sujetId = normalizeOptionalIntegerInput(existingSubjectRes.rows[0]?.id);
  const existingActionsRes = sujetId
    ? await actionPlanPool.query(
      `
      SELECT id
      FROM public.action
      WHERE sujet_id = $1
        AND LOWER(COALESCE(type, 'action')) = 'action'
      ORDER BY id ASC
      `,
      [sujetId]
    )
    : { rows: [] };

  const existingActionIds = new Set(
    existingActionsRes.rows
      .map((row) => normalizeOptionalIntegerInput(row.id))
      .filter((value) => Number.isInteger(value))
  );

  if (!meaningfulActions.length) {
    if (!sujetId || !existingActionIds.size) {
      return { savedCount: 0, deletedCount: 0 };
    }

    await actionPlanPool.query(
      `DELETE FROM public.action WHERE sujet_id = $1`,
      [sujetId]
    );
    await actionPlanPool.query(
      `DELETE FROM public.sujet WHERE id = $1`,
      [sujetId]
    );

    return {
      savedCount: 0,
      deletedCount: existingActionIds.size
    };
  }

  const ensuredSubject = await ensureActionPlanSubject({
    kpiTargetAllocationId,
    kpiId,
    periodLabel,
    subject,
    kpiName
  });
  sujetId = normalizeOptionalIntegerInput(ensuredSubject?.sujetId);
  if (!sujetId) {
    return { savedCount: 0, deletedCount: 0 };
  }

  let savedCount = 0;
  const keptActionIds = new Set();

  for (let index = 0; index < meaningfulActions.length; index += 1) {
    const action = meaningfulActions[index];
    const actionId = normalizeOptionalIntegerInput(
      action.correctiveActionId ??
      action.corrective_action_id ??
      action.id
    );
    const status =
      normalizeCorrectiveActionStatus(action.status, OPEN_CORRECTIVE_ACTION_STATUS) ||
      OPEN_CORRECTIVE_ACTION_STATUS;
    const responsibleName =
      normalizeOptionalTextInput(action.responsibleName ?? action.responsible) ||
      normalizeOptionalTextInput(responsibleContext?.name);
    const responsibleEmail = resolveActionPlanResponsibleEmail({
      responsibleName,
      responsibleContext
    });
    const values = [
      normalizeOptionalTextInput(action.rootCause ?? action.root_cause),
      normalizeOptionalTextInput(action.implementedSolution ?? action.implemented_solution),
      status,
      responsibleName,
      normalizeOptionalDateInput(action.dueDate ?? action.due_date),
      index + 1,
      responsibleEmail
    ];

    if (actionId && existingActionIds.has(actionId)) {
      const updatedActionRes = await actionPlanPool.query(
        `
        UPDATE public.action
        SET
          titre = $2,
          description = $3,
          status = $4,
          responsable = $5,
          due_date = $6,
          ordre = $7,
          email_responsable = $8,
          type = 'action',
          updated_at = NOW(),
          closed_date = CASE
            WHEN LOWER($4::text) = 'closed' THEN COALESCE(closed_date, CURRENT_DATE)
            ELSE NULL
          END
        WHERE id = $1
          AND sujet_id = $9
        RETURNING id
        `,
        [actionId, ...values, sujetId]
      );

      const updatedActionId = normalizeOptionalIntegerInput(updatedActionRes.rows[0]?.id);
      if (updatedActionId) {
        keptActionIds.add(updatedActionId);
        savedCount += 1;
        continue;
      }
    }

    const insertedActionRes = await actionPlanPool.query(
      `
    INSERT INTO public.action (
      sujet_id,
      type,
      titre,
      description,
      status,
      responsable,
      due_date,
     ordre,
     email_responsable,
     closed_date
    )
      VALUES (
        $1,
        'action',
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        CASE
          WHEN LOWER($4::text) = 'closed' THEN CURRENT_DATE
          ELSE NULL
        END
      )
      RETURNING id
      `,
      [sujetId, ...values]
    );

    const insertedActionId = normalizeOptionalIntegerInput(insertedActionRes.rows[0]?.id);
    if (insertedActionId) {
      keptActionIds.add(insertedActionId);
      savedCount += 1;
    }
  }

  const staleActionIds = Array.from(existingActionIds).filter(
    (id) => !keptActionIds.has(id)
  );

  if (staleActionIds.length) {
    await actionPlanPool.query(
      `DELETE FROM public.action WHERE sujet_id = $1 AND id = ANY($2::bigint[])`,
      [sujetId, staleActionIds]
    );
  }

  return {
    savedCount,
    deletedCount: staleActionIds.length
  };
};

const updateActionPlanCorrectiveActionStatus = async (correctiveActionId, nextStatus) => {
  const actionId = normalizeOptionalIntegerInput(correctiveActionId);
  if (!actionId) return null;

  const result = await actionPlanPool.query(
    `
    UPDATE public.action
    SET
      status = $2,
      updated_at = NOW(),
      closed_date = CASE
        WHEN LOWER($2::text) = 'closed' THEN COALESCE(closed_date, CURRENT_DATE)
        ELSE NULL
      END
    WHERE id = $1
    RETURNING id, status
    `,
    [actionId, nextStatus]
  );

  if (!result.rows.length) {
    return null;
  }

  return {
    corrective_action_id: result.rows[0].id,
    status:
      normalizeCorrectiveActionStatus(result.rows[0].status, nextStatus) ||
      nextStatus
  };
};

const getResponsibleWithKPIs = async (responsibleId, week) => {
  const responsible = await loadFormResponsibleContext(responsibleId);
  const normalizedWeek = normalizeOptionalTextInput(week);
  const responsibleNumericId = normalizeOptionalIntegerInput(
    responsible?.people_id ?? responsibleId
  );
  const correctiveActionsEnabled = await canUseActionPlanCorrectiveActions();

  const kpiRes = await pool.query(
    `
    SELECT
      kta.kpi_target_allocation_id AS kpi_values_id,
      kta.kpi_target_allocation_id,
      kta.kpi_id,
      COALESCE(subject_link.subject_name, k.kpi_sub_title, k.kpi_name, 'KPI') AS subject,
      COALESCE(k.kpi_name, k.kpi_sub_title, 'Untitled KPI') AS indicator_sub_title,
      COALESCE(kta.target_unit, k.unit) AS unit,
      COALESCE(latest_result.target_value, kta.target_value, k.target_value) AS target,
      k.min_value AS min,
      k.max_value AS max,
      k.tolerance_type,
      k.up_tolerance,
      k.low_tolerance,
      k.frequency,
      k.kpi_explanation AS definition,
      k.calculation_on,
      k.target_auto_adjustment,
      COALESCE(latest_result.upper_limit, k.high_limit) AS high_limit,
      COALESCE(latest_result.lower_limit, k.low_limit) AS low_limit,
      k.target_direction,
      latest_result.raw_value AS value,
      latest_result.comments AS latest_comment,
      COALESCE(latest_result.recorded_at, CURRENT_TIMESTAMP) AS last_updated
    FROM public.kpi_target_allocation kta
    JOIN public.kpi k ON k.kpi_id = kta.kpi_id
    LEFT JOIN LATERAL (
      SELECT s.subject_name
      FROM public.subject_kpi sk
      JOIN public.subject s ON s.subject_id = sk.subject_id
      WHERE sk.kpi_id = k.kpi_id
      ORDER BY COALESCE(sk.is_primary, false) DESC, sk.subject_kpi_id ASC
      LIMIT 1
    ) subject_link ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        kr.raw_value,
        kr.target_value,
        kr.upper_limit,
        kr.lower_limit,
        kr.comments,
        kr.recorded_at
      FROM public.kpi_result kr
      WHERE kr.kpi_target_allocation_id = kta.kpi_target_allocation_id
        AND (
          ($1::text IS NOT NULL AND (kr.period_label = $1 OR kr.week = $1))
          OR $1::text IS NULL
        )
      ORDER BY
        CASE WHEN $1::text IS NOT NULL AND (kr.period_label = $1 OR kr.week = $1) THEN 0 ELSE 1 END,
        kr.recorded_at DESC,
        kr.kpi_result_id DESC
      LIMIT 1
    ) latest_result ON TRUE
    WHERE $2::integer IS NOT NULL
      AND kta.set_by_people_id = $2
    ORDER BY
      COALESCE(subject_link.subject_name, k.kpi_sub_title, k.kpi_name, 'KPI') ASC,
      COALESCE(k.kpi_name, k.kpi_sub_title, 'Untitled KPI') ASC,
      kta.kpi_target_allocation_id ASC
    `,
    [normalizedWeek, responsibleNumericId]
  );

  const {
    currentByAllocationId,
    historyByAllocationId
  } = correctiveActionsEnabled
    ? await loadActionPlanCorrectiveActionMaps(kpiRes.rows, normalizedWeek)
    : { currentByAllocationId: {}, historyByAllocationId: {} };

  Object.values(historyByAllocationId).forEach((rows) => {
    (rows || []).forEach((action) => {
      if (!action) return;

      action.corrective_action_id =
        action.corrective_action_id ??
        action.id ??
        null;
    });
  });

  const kpis = kpiRes.rows.map((kpi) => {
    const allocationKey = String(
      kpi.kpi_target_allocation_id ?? kpi.kpi_values_id ?? ""
    ).trim();
    const currentPeriodActions = currentByAllocationId[allocationKey] || [];
    const openActions = currentPeriodActions.filter((action) => {
      return (
        normalizeCorrectiveActionStatus(action.status, OPEN_CORRECTIVE_ACTION_STATUS) !==
        CLOSED_CORRECTIVE_ACTION_STATUS
      );
    });

    const correctiveActions = sortCorrectiveActions(openActions);
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

  return {
    responsible,
    kpis,
    correctiveActionsEnabled,
    correctiveActionHistoryByAllocation: historyByAllocationId
  };
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

    // â”€â”€ value exceeds current target â†’ queue it, touch NOTHING else â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if (numValue > currentTarget) {
      console.log(`ðŸ“Œ KPI ${kpiId}: ${numValue} > target ${currentTarget} â€” queuing pending update`);

      await pool.query(
        `INSERT INTO public.pending_target_updates
           (kpi_id, responsible_id, week, new_target, applied)
         VALUES ($1, $2, $3, $4, false)
         ON CONFLICT (kpi_id, responsible_id, week)
         DO UPDATE SET new_target = EXCLUDED.new_target, applied = false`,
        [kpiId, responsibleId, week, String(numValue)]
      );

      console.log(`Pending target queued â€” KPI ${kpiId}: ${currentTarget} â†’ ${numValue}`);

      return {
        targetUpdated: true,
        updateInfo: { kpiId, oldTarget: currentTarget, newTarget: numValue }
      };
    }

    return { targetUpdated: false };

  } catch (error) {
    console.error('âŒ checkAndTriggerCorrectiveActions error:', error.message);
    return { targetUpdated: false, error: error.message };
  }
};

// ============================================================
// upsertCorrectiveAction â€” saves root_cause, implemented_solution,
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
      status: normalizeCorrectiveActionStatus(status)
    };
    let resolvedStatus = normalizedPayload.status || "Open";

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
          resolvedStatus
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
        resolvedStatus
      ]
    );

    return true;
  } catch (err) {
    console.error("upsertCorrectiveAction error:", err.message);
    return false;
  }
};
// ============================================================
// AI SUGGESTION HELPER â€” generates 2 CA suggestions for a KPI
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
CONFIRMED PAGE CONTEXT
${JSON.stringify({
      responsible: responsible?.name || null,
      plant: responsible?.plant_name || null,
      department: responsible?.department_name || null,
      week: week || null
    }, null, 2)}

SELECTED KPI
${selectedKpi ? JSON.stringify(selectedKpi, null, 2) : "None selected"}

SELECTED KPI SUMMARY
${JSON.stringify(selectedKpiSummary, null, 2)}

MATCHED KNOWLEDGE BASE NODES
${JSON.stringify(promptKnowledgeMatches, null, 2)}

RELATED KNOWLEDGE BASE NODES
${JSON.stringify(promptKnowledgeRelated, null, 2)}

USER MESSAGE
${message}

IMPORTANT
- Follow the system prompt exactly.
- Use conversation data first.
- Use the provided knowledge base nodes as diagnostic support.
- Use confirmed page/KPI context as supporting evidence.
- Do not invent causes, actions, owners, deadlines, metrics, or evidence.
`.trim();

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
      kpis,
      conversation_history
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
      message: cleanMessage,
      conversationHistory: Array.isArray(conversation_history) ? conversation_history : []
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
          COALESCE(ca.updated_date, ca.created_date) DESC NULLS LAST,
          ca.corrective_action_id DESC`,
      [responsible_id]
    );

    const actions = actionsRes.rows.filter(
      (action) =>
        normalizeCorrectiveActionStatus(action.status, OPEN_CORRECTIVE_ACTION_STATUS) !==
        CLOSED_CORRECTIVE_ACTION_STATUS
    );
    const latestSubmittedActionId = actions[0]?.corrective_action_id ?? null;

    const formatDate = (dateValue) => {
      if (!dateValue) return "â€”";
      const d = new Date(dateValue);
      if (isNaN(d.getTime())) return "â€”";
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
          <div class="empty-icon">ðŸ“­</div>
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
            : "â€”";

        const safeStatusClass = String(a.status || "open")
          .toLowerCase()
          .replace(/\s+/g, "-");

        const isLatestSubmitted =
          latestSubmittedActionId !== null &&
          String(a.corrective_action_id) === String(latestSubmittedActionId);

        return `
            <div class="ca-card ${isOverdue ? "overdue" : ""} ${isLatestSubmitted ? "latest-submitted" : ""}">
              <div class="ca-card-top">
                <div>
                  <div class="ca-kpi-title">${escapeHtml(a.indicator_title)}</div>
                  <div class="ca-kpi-subtitle-row">
                    <div class="ca-kpi-subtitle">Action ${escapeHtml(a.action_number || 1)}</div>
                    ${isLatestSubmitted ? `<span class="latest-submitted-badge">Latest Submitted</span>` : ""}
                  </div>
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
                  <strong>${a.value ?? "â€”"} ${escapeHtml(a.unit || "")}</strong>
                </div>
                <div class="stat-box">
                  <span class="stat-label">Target</span>
                  <strong>${a.target ?? "â€”"} ${escapeHtml(a.unit || "")}</strong>
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
                  <strong>${escapeHtml(a.responsible || responsible.name || "â€”")}</strong>
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
            ? (isOverdue ? "â›” Overdue" : "ðŸ“… Due date set")
            : "ðŸ•’ No due date"}
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
          .ca-card.latest-submitted{
            border:1px solid #93c5fd;
            box-shadow:0 12px 30px rgba(37,99,235,0.14);
            grid-column:1 / -1;
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
          .ca-kpi-subtitle-row{
            display:flex;
            align-items:center;
            gap:10px;
            flex-wrap:wrap;
            margin-top:6px;
          }
          .ca-kpi-subtitle-row .ca-kpi-subtitle{
            margin-top:0;
          }
          .latest-submitted-badge{
            display:inline-flex;
            align-items:center;
            padding:5px 10px;
            border-radius:999px;
            background:#dbeafe;
            color:#1d4ed8;
            font-size:11px;
            font-weight:800;
            letter-spacing:.3px;
            text-transform:uppercase;
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
            <h1>ðŸ“‹ Corrective Actions</h1>
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
              â† Back to Dashboard
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
                <strong>${escapeHtml(kpi.indicator_sub_title || "â€”")}</strong>
              </div>
              <div class="info-box">
                <span>Current Value</span>
                <strong>${kpi.value ?? "â€”"} ${escapeHtml(kpi.unit || "")}</strong>
              </div>
              <div class="info-box">
                <span>Target</span>
                <strong>${kpi.target ?? "â€”"} ${escapeHtml(kpi.unit || "")}</strong>
              </div>
              <div class="info-box">
                <span>Low Limit</span>
                <strong>${kpi.low_limit ?? "â€”"} ${escapeHtml(kpi.unit || "")}</strong>
              </div>
              <div class="info-box">
                <span>High Limit</span>
                <strong>${kpi.high_limit ?? "â€”"} ${escapeHtml(kpi.unit || "")}</strong>
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
                  <label for="due_date">ðŸ“… Due Date *</label>
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
                  <label for="responsible">ðŸ‘¤ Responsible *</label>
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
  <label for="root_cause">ðŸ” Root Cause Analysis *</label>
  <textarea id="root_cause" name="root_cause" required>${escapeHtml(ed.root_cause || "")}</textarea>
</div>

<div class="form-group">
  <label for="implemented_solution">ðŸ”§ Implemented Solution *</label>
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
      status: normalizeCorrectiveActionStatus(req.body?.status)
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

app.post("/api/corrective-actions/:correctiveActionId/status", async (req, res) => {
  try {
    const correctiveActionId = normalizeText(req.params.correctiveActionId);
    const nextStatus = normalizeCorrectiveActionStatus(req.body?.status);

    if (!correctiveActionId) {
      return res.status(400).json({ error: "Missing corrective action id" });
    }

    if (!nextStatus) {
      return res.status(400).json({ error: "Invalid corrective action status" });
    }

    if (await canUseActionPlanCorrectiveActions()) {
      const actionPlanResult = await updateActionPlanCorrectiveActionStatus(
        correctiveActionId,
        nextStatus
      );

      if (actionPlanResult) {
        return res.json(actionPlanResult);
      }
    }

    if (!(await hasPublicTable("corrective_actions"))) {
      return res.status(404).json({ error: "Corrective action not found" });
    }

    const result = await pool.query(
      `UPDATE public.corrective_actions
       SET status = $2::text,
           updated_date = NOW()
       WHERE corrective_action_id = $1
       RETURNING corrective_action_id, status`,
      [correctiveActionId, nextStatus]
    );

    if (!result.rows.length) {
      return res.status(404).json({ error: "Corrective action not found" });
    }

    res.json({
      corrective_action_id: result.rows[0].corrective_action_id,
      status: result.rows[0].status
    });
  } catch (err) {
    console.error("Error updating corrective action status:", err);
    res.status(500).json({ error: "Failed to update corrective action status" });
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
          <h2 style="color:#4caf50;">No Open Corrective Actions</h2>
          <p>All corrective actions for week ${week} have been completed.</p>
          <a href="/dashboard?responsible_id=${encodeURIComponent(responsible_id)}"
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
              <span class="ai-icon">&#129302;</span>
              <span class="ai-title">AI Corrective Action Suggestion</span>
              <button type="button" class="generate-btn"
                id="gen-btn-${action.corrective_action_id}"
                onclick="generateSuggestion('${action.corrective_action_id}','${action.kpi_id}','${responsible_id}','${week}')">
                <span class="gen-btn-icon">&#10024;</span>
                <span class="gen-btn-text">Generate Suggestion</span>
              </button>
            </div>

            <div class="suggestion-content" id="suggestion-${action.corrective_action_id}" style="display:none;">
              <div class="ai-suggestion-row">
                <div class="ai-suggestion-card root-cause-card"
                     onclick="applyToField('root_cause_${action.corrective_action_id}',this)">
                  <div class="ai-card-label">
                    <span class="ai-card-icon">&#128269;</span>Root Cause
                    <span class="apply-hint">Click to apply &#8595;</span>
                  </div>
                  <div class="ai-card-text" id="rc-text-${action.corrective_action_id}"></div>
                </div>
                <div class="ai-suggestion-card action-card"
                     onclick="applyToField('solution_${action.corrective_action_id}',this)">
                  <div class="ai-card-label">
                    <span class="ai-card-icon">&#9889;</span>Immediate Action
                    <span class="apply-hint">Click to apply &#8595;</span>
                  </div>
                  <div class="ai-card-text" id="ia-text-${action.corrective_action_id}"></div>
                </div>
              </div>
            </div>

            <div class="suggestion-error" id="error-${action.corrective_action_id}" style="display:none;">
              <span>&#9888;&#65039; Could not generate suggestion. Please try again or fill manually.</span>
            </div>
          </div>

          <!-- Form Fields -->
          <div class="form-fields">
            <div class="form-group">
              <label for="root_cause_${action.corrective_action_id}">
                &#128269; Root Cause <span class="required">*</span>
              </label>
              <textarea name="root_cause_${action.corrective_action_id}"
                        id="root_cause_${action.corrective_action_id}" required
                        placeholder="Click 'Generate Suggestion' above, or describe the root cause manually"
              >${action.root_cause || ''}</textarea>
            </div>
            <div class="form-group">
              <label for="solution_${action.corrective_action_id}">
                &#9889; Implemented Solution <span class="required">*</span>
              </label>
              <textarea name="solution_${action.corrective_action_id}"
                        id="solution_${action.corrective_action_id}" required
                        placeholder="Click 'Generate Suggestion' above, or describe actions taken manually"
              >${action.implemented_solution || ''}</textarea>
            </div>

            <div class="form-grid">
              <div class="form-group">
                <label for="due_date_${action.corrective_action_id}">
                  &#128197; Due Date <span class="required">*</span>
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
                  &#128100; Responsible <span class="required">*</span>
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
        <title>Corrective Actions â€” Week ${week}</title>
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
          .ai-suggestion-row{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;}
          .ai-suggestion-card{background:white;border-radius:8px;padding:14px;cursor:pointer;
            transition:transform 0.15s,box-shadow 0.15s;border:1.5px solid transparent;}
          .ai-suggestion-card:hover{transform:translateY(-2px);box-shadow:0 6px 16px rgba(0,0,0,0.1);}
          .ai-suggestion-card.applied{border-color:#4ade80!important;background:#f0fdf4;}
          .root-cause-card{border-top:3px solid #ef4444;}
          .action-card{border-top:3px solid #f59e0b;}
          .ai-card-label{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;
            margin-bottom:8px;display:flex;align-items:center;gap:5px;}
          .root-cause-card .ai-card-label{color:#dc2626;}
          .action-card .ai-card-label{color:#d97706;}
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
            <div class="header-icon">âš ï¸</div>
            <h1>Corrective Actions Required</h1>
            <div class="header-badge">
              ${actions.length} KPI${actions.length > 1 ? 's' : ''} Below Target â€” Week ${week}
            </div>
          </div>
          <div class="responsible-bar">
            ðŸ‘¤ <strong>${responsible.name}</strong> &nbsp;â€¢&nbsp;
            ðŸ­ ${responsible.plant_name} &nbsp;â€¢&nbsp;
            ðŸ·ï¸ ${responsible.department_name}
          </div>

          <div class="form-section">
            <form action="/submit-bulk-corrective-actions" method="POST">
              <input type="hidden" name="responsible_id" value="${responsible_id}">
              <input type="hidden" name="week" value="${week}">
              ${kpiSectionsHtml}
              <button type="submit" class="submit-btn">
                âœ“ Submit All Corrective Actions (${actions.length})
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
            if (hint) hint.textContent = 'âœ“ Applied';
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
              if (hint) hint.textContent = 'Click to apply \u2193';
            });

            btn.disabled = true;
            btn.classList.add('loading');
            btn.querySelector('.gen-btn-icon').textContent = '\u23F3';
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
              suggDiv.style.display = 'block';
            } catch (err) {
              errDiv.style.display = 'block';
            } finally {
              btn.disabled = false;
              btn.classList.remove('loading');
              btn.querySelector('.gen-btn-icon').textContent = '\u{1F504}';
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
      const dueDate = formData[`due_date_${caId}`] || null;
      const responsibleName = normalizeText(formData[`responsible_${caId}`]);
      if (rootCause || solution || dueDate || responsibleName) {
        await pool.query(
          `UPDATE public.corrective_actions
           SET root_cause=$1,
               implemented_solution=$2,
               due_date=$3::date,
               responsible=$4::text,
               status = CASE
                 WHEN $1::text IS NOT NULL
                  AND $2::text IS NOT NULL
                  AND $3::date IS NOT NULL
                  AND NULLIF(BTRIM($4::text), '') IS NOT NULL
                 THEN 'Waiting for validation'
                 ELSE status
               END,
               updated_date=NOW()
           WHERE corrective_action_id=$5`,
          [rootCause, solution, dueDate, responsibleName, caId]
        );
        if (rootCause && solution && dueDate && responsibleName) {
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
        <h1>All Corrective Actions Submitted!</h1>
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
    const normalizedWeek = normalizeOptionalTextInput(week) || formatIsoDateValue(new Date());
    const resultDate = deriveKpiResultDate(normalizedWeek);
    const periodType = deriveKpiResultPeriodType(normalizedWeek);
    const recordedByPeopleId = await resolveRecordedByPeopleId(responsible_id);
    const responsibleContext = await loadFormResponsibleContext(responsible_id);
    const correctiveActionsEnabled = await canUseActionPlanCorrectiveActions();

    // Extract KPI values
    const kpiValues = Object.entries(values)
      .filter(([k]) => k.startsWith("value_"))
      .map(([k, v]) => ({ kpi_values_id: k.split("_")[1], value: v }));

    // Extract comments
    const comments = Object.entries(values)
      .filter(([k]) => k.startsWith("comment_"))
      .reduce((acc, [k, v]) => { acc[k.split("_")[1]] = v; return acc; }, {});

    const defaultResponsibleName = responsibleContext.name || null;
    const targetUpdates = [];
    let correctiveActionsCount = 0;

    for (const item of kpiValues) {
      const allocationId = normalizeOptionalIntegerInput(item.kpi_values_id);
      const rawValueText = normalizeOptionalTextInput(item.value);
      if (!allocationId || rawValueText === null) continue;

      const normalizedValue = normalizeOptionalNumericInput(rawValueText);
      if (normalizedValue === null) {
        throw createHttpError(400, "All KPI values must be numeric.");
      }

      const numericValue = Number(normalizedValue);
      const allocationRes = await pool.query(
        `
        SELECT
          kta.kpi_target_allocation_id,
          kta.kpi_id,
          kta.kpi_type,
          kta.last_best_target,
          COALESCE(kta.target_value, k.target_value) AS effective_target_value,
          COALESCE(kta.target_unit, k.unit) AS effective_unit,
          k.low_limit,
          k.high_limit,
          k.target_direction,
          COALESCE(k.kpi_name, k.kpi_sub_title, 'KPI') AS kpi_name,
          COALESCE(k.kpi_sub_title, k.kpi_name, 'KPI') AS subject_name
        FROM public.kpi_target_allocation kta
        JOIN public.kpi k ON k.kpi_id = kta.kpi_id
        WHERE kta.kpi_target_allocation_id = $1
        LIMIT 1
        `,
        [allocationId]
      );
      if (!allocationRes.rows.length) continue;

      const allocation = allocationRes.rows[0];
      const kpiId = allocation.kpi_id;
      const submittedActions = correctiveActionsEnabled
        ? getSubmittedCorrectiveActions(
          values,
          item.kpi_values_id,
          defaultResponsibleName
        )
        : [];
      const commentText = normalizeOptionalTextInput(comments[item.kpi_values_id]);
      const existingResultRes = await pool.query(
        `
        SELECT kpi_result_id
        FROM public.kpi_result
        WHERE kpi_target_allocation_id = $1
          AND kpi_id = $2
          AND COALESCE(period_label, '') = $3
        ORDER BY recorded_at DESC, kpi_result_id DESC
        LIMIT 1
        `,
        [allocationId, kpiId, normalizedWeek]
      );
      const existingResultId = existingResultRes.rows[0]?.kpi_result_id || null;

      if (existingResultId) {
        await pool.query(
          `
          UPDATE public.kpi_result
          SET
            result_date = $2,
            week = $3,
            period_type = $4,
            period_label = $5,
            raw_value = $6,
            lower_limit = $7,
            upper_limit = $8,
            target_value = $9,
            best_new_target = $10,
            unit = $11,
            kpi_type = $12,
            result_status = $13,
            data_source = $14,
            recorded_by_people_id = $15,
            recorded_at = CURRENT_TIMESTAMP,
            comments = $16
          WHERE kpi_result_id = $1
          `,
          [
            existingResultId,
            resultDate,
            normalizedWeek,
            periodType,
            normalizedWeek,
            numericValue,
            allocation.low_limit,
            allocation.high_limit,
            allocation.effective_target_value,
            allocation.last_best_target,
            allocation.effective_unit,
            allocation.kpi_type,
            "Recorded",
            "Manual form",
            recordedByPeopleId,
            commentText
          ]
        );
      } else {
        await pool.query(
          `
          INSERT INTO public.kpi_result (
            kpi_target_allocation_id,
            kpi_id,
            result_date,
            week,
            period_type,
            period_label,
            raw_value,
            lower_limit,
            upper_limit,
            target_value,
            best_new_target,
            unit,
            kpi_type,
            result_status,
            data_source,
            recorded_by_people_id,
            comments
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
          `,
          [
            allocationId,
            kpiId,
            resultDate,
            normalizedWeek,
            periodType,
            normalizedWeek,
            numericValue,
            allocation.low_limit,
            allocation.high_limit,
            allocation.effective_target_value,
            allocation.last_best_target,
            allocation.effective_unit,
            allocation.kpi_type,
            "Recorded",
            "Manual form",
            recordedByPeopleId,
            commentText
          ]
        );
      }

      // -------------------------------------------------------
      // Save the corrective actions for this KPI period in the Action Plan DB.
      // -------------------------------------------------------
      if (correctiveActionsEnabled) {
        const syncResult = await syncActionPlanCorrectiveActionsForPeriod({
          kpiTargetAllocationId: allocationId,
          kpiId,
          periodLabel: normalizedWeek,
          subject: allocation.subject_name,
          kpiName: allocation.kpi_name,
          actions: submittedActions,
          responsibleContext
        });

        correctiveActionsCount += syncResult.savedCount || 0;
      }
    }

    let notifications = ["KPI results saved successfully"];
    if (targetUpdates.length > 0)
      notifications.push(`ðŸŽ¯ <strong>${targetUpdates.length} KPI target${targetUpdates.length > 1 ? 's' : ''} updated</strong>`);
    if (correctiveActionsCount > 0)
      notifications.push(`ðŸ“‹ <strong>${correctiveActionsCount} corrective action${correctiveActionsCount > 1 ? 's' : ''} recorded</strong>`);
    if (notifications.length === 0) notifications.push(` All KPIs are within targets`);

    res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>KPI Submitted</title>
      <style>body{font-family:'Segoe UI',sans-serif;background:#f4f4f4;display:flex;
        justify-content:center;align-items:center;height:100vh;margin:0;}
      .sc{background:#fff;padding:40px;border-radius:10px;text-align:center;max-width:600px;}
      .ni{display:flex;align-items:center;margin:10px 0;padding:10px;background:white;border-radius:6px;}
      .btn{display:inline-block;padding:12px 25px;background:#0078D7;color:white;
           text-decoration:none;border-radius:6px;font-weight:bold;margin:5px;}</style></head>
      <body><div class="sc">
        <h1 style="color:#28a745;">KPI Submitted Successfully!</h1>
        <p>Your KPI values for ${normalizedWeek} have been saved.</p>
     
        <a href="/dashboard?responsible_id=${encodeURIComponent(responsible_id)}" class="btn">Go to Dashboard</a>
      </div></body></html>`);
  } catch (err) {
    res.status(err.statusCode || 500).send(`<h2 style="color:red;">âŒ Failed: ${err.message}</h2>`);
  }
});



app.get("/api/kpi-chart-data", async (req, res) => {
  try {
    const { kpi_id, week, kpi_values_id } = req.query;
    const allocationId = normalizeOptionalIntegerInput(kpi_values_id);
    const kpiId = normalizeOptionalIntegerInput(kpi_id);
    const normalizedWeek = normalizeOptionalTextInput(week);

    if ((!allocationId && !kpiId) || !normalizedWeek) {
      return res.status(400).json({ error: "Missing kpi_values_id or kpi_id, or week" });
    }

    const baseWhereClause = allocationId
      ? "kr.kpi_target_allocation_id = $1"
      : "kr.kpi_id = $1";
    const baseWhereValue = allocationId || kpiId;

    const histRes = await pool.query(
      `
      SELECT DISTINCT ON (COALESCE(kr.week, kr.period_label, TO_CHAR(kr.result_date, 'YYYY-MM-DD')))
        COALESCE(kr.week, kr.period_label, TO_CHAR(kr.result_date, 'YYYY-MM-DD')) AS week,
        kr.raw_value AS new_value,
        kr.recorded_at AS updated_at
      FROM public.kpi_result kr
      WHERE ${baseWhereClause}
        AND kr.raw_value IS NOT NULL
      ORDER BY
        COALESCE(kr.week, kr.period_label, TO_CHAR(kr.result_date, 'YYYY-MM-DD')),
        kr.recorded_at DESC,
        kr.kpi_result_id DESC
      `,
      [baseWhereValue]
    );

    const currentRes = await pool.query(
      `
      SELECT kr.raw_value AS value
      FROM public.kpi_result kr
      WHERE ${baseWhereClause}
        AND (kr.period_label = $2 OR kr.week = $2)
      ORDER BY kr.recorded_at DESC, kr.kpi_result_id DESC
      LIMIT 1
      `,
      [baseWhereValue, normalizedWeek]
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

    const currentMonthLabel = weekToMonthLabel(normalizedWeek);
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
    const {
      responsible,
      kpis,
      correctiveActionsEnabled,
      correctiveActionHistoryByAllocation
    } = await getResponsibleWithKPIs(responsible_id, week);

    if (!kpis.length) {
      return res.send(`
        <!DOCTYPE html>
        <html>
        <head>
          <meta charset="utf-8">
          <title>No KPI Targets</title>
          <style>
            body{font-family:'Segoe UI',sans-serif;background:#f4f6f9;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;padding:24px;}
            .empty-state{max-width:620px;background:#fff;border-radius:18px;padding:32px;box-shadow:0 20px 50px rgba(15,23,42,0.08);border:1px solid #e2e8f0;}
            h1{margin:0 0 12px;font-size:28px;color:#0f172a;}
            p{margin:0 0 12px;color:#475569;line-height:1.6;}
            code{background:#eef2ff;padding:2px 6px;border-radius:6px;color:#1d4ed8;}
          </style>
        </head>
        <body>
          <div class="empty-state">
            <h1>No KPI values to enter yet</h1>
            <p>The <code>/form</code> page now shows only KPI target allocations where <code>set_by_people_id</code> matches the current <code>responsible_id</code>.</p>
            <p>No target allocations are currently assigned to this responsible. Set the responsible in <code>kpi_target_allocation.set_by_people_id</code>, then this page will display only that person’s related KPIs and save submitted values into <code>kpi_result</code>.</p>
          </div>
        </body>
        </html>
      `);
    }

    const allocationIds = kpis
      .map((kpi) => normalizeOptionalIntegerInput(kpi.kpi_values_id))
      .filter((value) => Number.isInteger(value));

    const histRes = await pool.query(
      `
      SELECT DISTINCT ON (kr.kpi_target_allocation_id, COALESCE(kr.week, kr.period_label, TO_CHAR(kr.result_date, 'YYYY-MM-DD')))
        kr.kpi_target_allocation_id AS kpi_values_id,
        kr.kpi_id,
        COALESCE(kr.week, kr.period_label, TO_CHAR(kr.result_date, 'YYYY-MM-DD')) AS week,
        kr.raw_value AS new_value,
        kr.recorded_at AS updated_at
      FROM public.kpi_result kr
      WHERE kr.kpi_target_allocation_id = ANY($1::int[])
        AND kr.raw_value IS NOT NULL
      ORDER BY
        kr.kpi_target_allocation_id,
        COALESCE(kr.week, kr.period_label, TO_CHAR(kr.result_date, 'YYYY-MM-DD')),
        kr.recorded_at DESC,
        kr.kpi_result_id DESC
      `,
      [allocationIds]
    );

    const historyByKpi = {};
    histRes.rows.forEach((row) => {
      const historyKey = String(row.kpi_values_id || row.kpi_id);
      if (!historyByKpi[historyKey]) historyByKpi[historyKey] = [];
      historyByKpi[historyKey].push({
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

    function sortHistoryEntries(entries, getUpdatedAt) {
      return (Array.isArray(entries) ? entries.slice() : []).sort((a, b) => {
        const weekDiff = weekLabelToDate(b?.week) - weekLabelToDate(a?.week);
        if (weekDiff !== 0) return weekDiff;

        return new Date(getUpdatedAt(b) || 0) - new Date(getUpdatedAt(a) || 0);
      });
    }

    const encodeModalPayload = (value) =>
      encodeURIComponent(JSON.stringify(value ?? null));

    const commentsHistoryRes = await pool.query(
      `
      SELECT DISTINCT ON (kr.kpi_target_allocation_id, COALESCE(kr.week, kr.period_label, TO_CHAR(kr.result_date, 'YYYY-MM-DD')))
        kr.kpi_target_allocation_id AS kpi_values_id,
        kr.kpi_id,
        COALESCE(kr.week, kr.period_label, TO_CHAR(kr.result_date, 'YYYY-MM-DD')) AS week,
        kr.comments AS comment,
        kr.recorded_at AS updated_at
      FROM public.kpi_result kr
      WHERE kr.kpi_target_allocation_id = ANY($1::int[])
        AND kr.comments IS NOT NULL
        AND BTRIM(kr.comments) <> ''
      ORDER BY
        kr.kpi_target_allocation_id,
        COALESCE(kr.week, kr.period_label, TO_CHAR(kr.result_date, 'YYYY-MM-DD')),
        kr.recorded_at DESC,
        kr.kpi_result_id DESC
      `,
      [allocationIds]
    );
    const correctiveActionsHistoryRows = Object.values(
      correctiveActionHistoryByAllocation || {}
    ).flat();

    const commentsByKpiMonth = {};
    commentsHistoryRes.rows.forEach((row) => {
      const monthLabel = weekToMonthLabel(row.week);
      const commentKey = String(row.kpi_values_id || row.kpi_id);
      if (!commentsByKpiMonth[commentKey]) commentsByKpiMonth[commentKey] = {};
      if (!commentsByKpiMonth[commentKey][monthLabel]) commentsByKpiMonth[commentKey][monthLabel] = [];
      commentsByKpiMonth[commentKey][monthLabel].push({
        week: row.week,
        month_label: monthLabel,
        text: row.comment,
        updated_at: row.updated_at
      });
    });

    const correctiveActionsByKpiMonth = {};
    correctiveActionsHistoryRows.forEach((row) => {
      const historyKey = String(
        row.kpi_values_id ??
        row.kpi_target_allocation_id ??
        row.kpi_id ??
        ""
      ).trim();
      if (!historyKey) return;

      const periodLabel = row.week || row.period_label || "";
      const monthLabel = weekToMonthLabel(periodLabel);
      const updatedAt = row.updated_date || row.created_date || null;

      if (!correctiveActionsByKpiMonth[historyKey]) {
        correctiveActionsByKpiMonth[historyKey] = {};
      }

      if (!correctiveActionsByKpiMonth[historyKey][monthLabel]) {
        correctiveActionsByKpiMonth[historyKey][monthLabel] = [];
      }

      correctiveActionsByKpiMonth[historyKey][monthLabel].push({
        corrective_action_id: row.corrective_action_id || null,
        id: row.id || row.corrective_action_id || null,
        kpi_values_id: historyKey,
        week: periodLabel,
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
      const showCA = correctiveActionsEnabled && currentValue !== null &&
        needsCorrectiveAction(currentValue, lowLimit, highLimit, goodDirection);
      const correctiveActions = correctiveActionsEnabled ? sortCorrectiveActions(
        Array.isArray(kpi.corrective_actions) ? kpi.corrective_actions : []
      ) : [];
      const latestCorrectiveAction = getLatestCorrectiveAction(correctiveActions);
      const caStatus = latestCorrectiveAction?.status || "";
      const safeCaStatusClass = String(caStatus || "").toLowerCase().replace(/\s+/g, "-");
      const correctiveActionsToRender = correctiveActionsEnabled
        ? (correctiveActions.length ? correctiveActions : [{
        responsible: responsible.name || ""
      }])
        : [];
      const correctiveActionsHtml = correctiveActionsToRender
        .map((action, actionIndex) => buildCorrectiveActionEntryHtml({
          kpiValuesId: kpi.kpi_values_id,
          actionIndex,
          action,
          defaultResponsibleName: responsible.name || "",
          showRequired: showCA
        }))
        .join("");

      // â”€â”€ Corrective actions section (no footer add-btn inside) â”€â”€


      const rawHistory = historyByKpi[String(kpi.kpi_values_id || kpi.kpi_id)] || [];
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

      const allHistoryActions = sortHistoryEntries(
        Object.values(
          correctiveActionsByKpiMonth[kpi.kpi_id] ||
          correctiveActionsByKpiMonth[String(kpi.kpi_values_id)] ||
          {}
        ).flat(),
        (entry) => entry?.updated_at
      );
      const allHistoryComments = sortHistoryEntries(
        Object.values(
          commentsByKpiMonth[String(kpi.kpi_values_id)] ||
          commentsByKpiMonth[kpi.kpi_id] ||
          {}
        ).flat(),
        (entry) => entry?.updated_at
      );
      const hasHistoryDetails = Boolean(
        (correctiveActionsEnabled && allHistoryActions && allHistoryActions.length) ||
        (allHistoryComments && allHistoryComments.length)
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
      data-history-actions="${encodeModalPayload(allHistoryActions)}"
      data-history-comments="${encodeModalPayload(allHistoryComments)}">

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
                     <!-- â”€â”€ Manager Comment (moved above action bar) â”€â”€ -->
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

                <div class="kpi-history-panel ${hasHistoryDetails ? "history-available" : "history-empty"}">
                  <div class="kpi-history-copy">
                    ${correctiveActionsEnabled
                      ? ""
                      : '<div class="view-ca-note">Enter the KPI value for this period. The form will save it directly into the <strong>kpi_result</strong> table.</div>'}
                  </div>
                  ${correctiveActionsEnabled ? `<button
                    type="button"
                    class="view-ca-btn"
                    data-kpi-values-id="${kpi.kpi_values_id}"
                    onclick="openHistoryModal('${kpi.kpi_values_id}'); return false;">
                    <span>View Corrective Action</span>
                    <span class="view-ca-btn-icon" aria-hidden="true">&#8599;</span>
                  </button>` : ""}
                </div>
                    <!-- â”€â”€ Unified card action bar â”€â”€ -->
        <div class="kpi-card-actions">
       <div class="kpi-card-actions-left">
       <button
        type="button"
        class="card-action-btn card-action-btn--ai"
        onclick="openAssistantForKpi('${kpi.kpi_values_id}')">
        <span class="ai-btn-glow"></span>
        <span class="ai-btn-shine"></span>
        <span class="ai-btn-icon" aria-hidden="true">&#129302;</span>
        <span class="ai-btn-text">AI Support</span>
       </button>
  </div>

  <div class="kpi-card-actions-right">
    ${correctiveActionsEnabled ? `<button
      type="button"
      class="card-action-btn card-action-btn--primary open-ca-modal-btn"
      data-kpi-values-id="${kpi.kpi_values_id}"
      onclick="openCaTableModal('${kpi.kpi_values_id}'); return false;">
      <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round">
        <path d="M10 4v12M4 10h12"/>
      </svg>
      Add Action
    </button>` : ""}
  </div>
</div>
              </div>

              <div class="kpi-mini-stats">
                <div class="mini-stat-card">
                  <div class="mini-stat-label">HIGH LIMIT</div>
                  <div class="mini-stat-value high">${highLimit !== null ? highLimit : "â€”"}</div>
                  <div class="mini-stat-unit">${kpi.unit || ""}</div>
                </div>
                <div class="mini-stat-card">
                  <div class="mini-stat-label">TARGET</div>
                  <div class="mini-stat-value target">${targetValue !== null ? targetValue : "â€”"}</div>
                  <div class="mini-stat-unit">${kpi.unit || ""}</div>
                </div>
                <div class="mini-stat-card">
                  <div class="mini-stat-label">LOW LIMIT</div>
                  <div class="mini-stat-value low">${lowLimit !== null ? lowLimit : "â€”"}</div>
                  <div class="mini-stat-unit">${kpi.unit || ""}</div>
                </div>
              </div>
            </div>
          </div>
         ${correctiveActionsEnabled ? `<div class="ca-actions-stack" data-kpi-values-id="${kpi.kpi_values_id}" style="display:none;">
         ${correctiveActionsHtml}
           </div>` : ""}
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
            display:grid;
            grid-template-columns:repeat(4,minmax(0,1fr));
            gap:16px;
            background:linear-gradient(135deg,#f8fbff 0%,#eef6ff 100%);
            padding:20px;
            border-radius:18px;
            margin-bottom:25px;
            border:1px solid #cfe3fb;
            box-shadow:inset 0 1px 0 rgba(255,255,255,0.85);
          }
          .info-row{
            display:flex;
            flex-direction:column;
            gap:8px;
            align-items:stretch;
            min-width:0;
            margin-bottom:0;
          }
          .info-label{
            font-weight:800;
            color:#1d4ed8;
            font-size:12px;
            letter-spacing:.8px;
            text-transform:uppercase;
          }
          .info-value{
            min-width:0;
            padding:12px 14px;
            background:white;
            border:1px solid #d8e3f0;
            border-radius:12px;
            box-shadow:0 4px 12px rgba(15,23,42,0.04);
            font-size:15px;
            font-weight:700;
            color:#0f172a;
          }

          /* â”€â”€ KPI Card â”€â”€ */
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
            grid-template-columns:minmax(0,1fr) 380px;
            gap:20px;
            align-items:stretch;
            margin-top:16px;
            position:relative;
            isolation:isolate;
          }
          .kpi-left-panel,.kpi-right-panel{
            background:#fafafa;
            border:1px solid #e5e7eb;
            border-radius:20px;
            padding:18px;
            min-width:0;
            position:relative;
          }
          .kpi-left-panel{
            min-height:480px;
            display:flex;
            align-items:center;
            justify-content:center;
            position:relative;
            overflow:hidden;
            z-index:30;
            isolation:isolate;
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
        position:relative;
        z-index:31;

        /* ðŸ‘‡ ADD THIS */
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
           width:calc(100% - 12px);
           margin:0 0 0 auto;
           padding:14px 0 0;   /* â¬…ï¸ remove side padding */
           border:none;        /* â¬…ï¸ remove border */
           background:transparent; /* â¬…ï¸ remove background */
           display:flex;
           flex-direction:column;
           gap:14px;
           position:relative;
           z-index:32;
           box-sizing:border-box;
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
          position:relative;
          z-index:40;
          pointer-events:auto;
         box-shadow:0 8px 18px rgba(15,108,189,0.18); /* â¬…ï¸ softer */
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
            min-height:280px;display:flex;align-items:center;justify-content:center;position:relative;overflow:hidden;z-index:1;
          }
          .kpi-chart-trigger{
            cursor:zoom-in;transition:border-color 0.2s ease,box-shadow 0.2s ease;pointer-events:none;
          }
          .kpi-chart-trigger:hover{border-color:#bfdbfe;box-shadow:0 12px 28px rgba(37,99,235,0.10);}
          .chart-expand-btn{
            position:absolute;top:14px;right:14px;width:42px;height:42px;
            border:none;border-radius:14px;
            display:inline-flex;align-items:center;justify-content:center;
            background:rgba(255,255,255,0.92);color:#1d4ed8;
            box-shadow:0 12px 24px rgba(15,23,42,0.12);
            cursor:pointer;z-index:3;pointer-events:auto;
            transition:transform 0.18s ease,box-shadow 0.18s ease;
          }
          .chart-expand-btn:hover{transform:translateY(-1px);background:#eff6ff;}
          .chart-expand-btn svg{width:18px;height:18px;stroke:currentColor;stroke-width:2;fill:none;stroke-linecap:round;stroke-linejoin:round;}
          .kpi-right-panel canvas{width:100% !important;height:380px !important;max-width:100%;display:block;pointer-events:none;}

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

.history-filter-bar{
  display:flex;
  flex-wrap:wrap;
  align-items:flex-end;
  gap:12px;
  margin:0 0 14px;
}

.history-filter-group{
  display:flex;
  flex-direction:column;
  gap:6px;
  min-width:180px;
  flex:1 1 180px;
}

.history-filter-label{
  font-size:11px;
  font-weight:800;
  text-transform:uppercase;
  letter-spacing:0.05em;
  color:#64748b;
}

.history-filter-input{
  min-height:42px;
  padding:10px 12px;
  border:1px solid #cbd5e1;
  border-radius:12px;
  background:#fff;
  color:#0f172a;
  font:inherit;
  transition:border-color .2s ease, box-shadow .2s ease;
}

.history-filter-input:focus{
  outline:none;
  border-color:#93c5fd;
  box-shadow:0 0 0 4px rgba(59,130,246,0.12);
}

.history-filter-clear{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  min-height:42px;
  padding:10px 16px;
  border:none;
  border-radius:12px;
  background:#e2e8f0;
  color:#0f172a;
  font-size:12px;
  font-weight:800;
  cursor:pointer;
  transition:transform .2s ease, box-shadow .2s ease, background-color .2s ease, opacity .2s ease;
}

.history-filter-clear:hover{
  transform:translateY(-1px);
  background:#cbd5e1;
  box-shadow:0 10px 20px rgba(15,23,42,0.08);
}

.history-filter-clear:disabled{
  opacity:0.62;
  cursor:not-allowed;
  transform:none;
  box-shadow:none;
}

          /* â”€â”€ CA Section â”€â”€ */
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

          /* â”€â”€ Comment Section â”€â”€ */
          .comment-section{margin-top:16px;}
          .comment-section{position:relative;z-index:32;}
          .comment-label{font-weight:600;color:#555;margin-bottom:8px;font-size:13px;}
          .comment-input{
            width:100%;padding:10px;border:1px solid #ddd;border-radius:4px;
            min-height:70px;resize:vertical;font-family:inherit;box-sizing:border-box;
          }

          /* â”€â”€ Unified Card Action Bar â”€â”€ */
          .kpi-card-actions {
           display: flex;
           align-items: center;
           justify-content: flex-start; /* â¬…ï¸ instead of space-between */
           gap: 10px; /* â¬…ï¸ space between buttons */
           margin-top: 16px;
           padding-top: 14px;
           border-top: 1px solid #e5e7eb;
           flex-wrap: nowrap; /* â¬…ï¸ keep them on same row */
           position:relative;
           z-index:35;
           }
          .kpi-card-actions-left{display:flex;align-items:center;gap:10px;flex-wrap:wrap;}
          .kpi-card-actions-right{display:flex;align-items:center;gap:10px;}

          .card-action-btn{
            display:inline-flex;align-items:center;gap:7px;
            border:none;border-radius:10px;
            padding:9px 16px;
            font-size:13px;font-weight:700;cursor:pointer;
            position:relative;
            z-index:40;
            pointer-events:auto;
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

          /* â”€â”€ CA Table Modal â”€â”€ */
        .ca-modal-overlay {
        position: fixed;
        inset: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        background: rgba(15, 23, 42, 0.55);
        opacity: 0;
        visibility: hidden;
        pointer-events: none;
        z-index: 9999;
        transition: opacity 0.25s ease, visibility 0.25s ease;
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
          .ca-modal-overlay.active {
            opacity: 1;
            visibility: visible;
            pointer-events: auto;
          }
          .ca-modal-overlay.active .ca-modal-box{transform:translateY(0) scale(1);}
          .ca-modal-panel {
           width: min(1100px, 92vw);
           max-height: 90vh;
           overflow: auto;
           background: #fff;
           border-radius: 18px;
           transform: translateY(12px);
           transition: transform 0.25s ease;
           }

          .ca-modal-overlay.active .ca-modal-panel {
           transform: translateY(0);
           }
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

          /* â”€â”€ Modals (chart + history) â”€â”€ */
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
          .history-status-select{
            min-width:170px;
            padding:9px 36px 9px 12px;
            border-radius:999px;
            border:1px solid #cbd5e1;
            font-size:12px;
            font-weight:800;
            font-family:inherit;
            background-color:#ffffff;
            background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 20 20' fill='none' stroke='%2364758b' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='m5 8 5 5 5-5'/%3E%3C/svg%3E");
            background-position:right 12px center;
            background-repeat:no-repeat;
            background-size:14px;
            appearance:none;
            cursor:pointer;
            transition:border-color .2s ease, box-shadow .2s ease, background-color .2s ease;
          }
          .history-status-select:focus{
            outline:none;
            border-color:#93c5fd;
            box-shadow:0 0 0 4px rgba(59,130,246,0.12);
          }
          .history-status-select:disabled{opacity:0.72;cursor:wait;}
          .history-status-select.status-open{background-color:#fef2f2;color:#b91c1c;border-color:#fecaca;}
          .history-status-select.status-waiting-for-validation{background-color:#fff7ed;color:#c2410c;border-color:#fed7aa;}
          .history-status-select.status-completed,.history-status-select.status-closed{background-color:#ecfdf5;color:#047857;border-color:#a7f3d0;}
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

          /* â”€â”€ Global Loading / Submit Modal â”€â”€ */
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
          .close-action-confirm-overlay{
            position:fixed;inset:0;display:flex;align-items:center;justify-content:center;
            padding:20px;background:rgba(15,23,42,0.62);backdrop-filter:blur(8px);
            z-index:10005;opacity:0;pointer-events:none;transition:opacity 0.24s ease;
          }
          .close-action-confirm-overlay.active{opacity:1;pointer-events:auto;}
          .close-action-confirm-card{
            width:min(92vw,470px);background:#fff;border-radius:24px;overflow:hidden;
            border:1px solid #fecaca;box-shadow:0 28px 80px rgba(15,23,42,0.32);
            transform:translateY(18px) scale(0.97);transition:transform 0.24s ease;
          }
          .close-action-confirm-overlay.active .close-action-confirm-card{transform:translateY(0) scale(1);}
          .close-action-confirm-hero{
            padding:24px 24px 18px;
            background:
              radial-gradient(circle at top right, rgba(239,68,68,0.18), transparent 34%),
              linear-gradient(135deg,#fff5f5 0%,#fff7ed 100%);
            border-bottom:1px solid #fee2e2;
          }
          .close-action-confirm-badge{
            display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;
            background:#ffffff;border:1px solid #fecaca;color:#b91c1c;font-size:11px;
            font-weight:900;letter-spacing:0.08em;text-transform:uppercase;
            box-shadow:0 6px 18px rgba(239,68,68,0.08);
          }
          .close-action-confirm-title{
            margin:14px 0 8px;font-size:24px;font-weight:900;line-height:1.2;color:#111827;
          }
          .close-action-confirm-text{
            margin:0;font-size:15px;line-height:1.7;color:#475569;
          }
          .close-action-confirm-body{padding:22px 24px 24px;}
          .close-action-confirm-note{
            padding:14px 16px;border-radius:16px;background:#fff7ed;border:1px solid #fed7aa;
            color:#9a3412;font-size:13px;line-height:1.65;
          }
          .close-action-confirm-actions{
            display:flex;justify-content:flex-end;gap:12px;margin-top:20px;flex-wrap:wrap;
          }
          .close-action-confirm-cancel{
            padding:12px 18px;border:1px solid #dbe3ee;border-radius:14px;background:#ffffff;
            color:#334155;font-size:14px;font-weight:800;cursor:pointer;
            transition:background 0.18s ease,border-color 0.18s ease,transform 0.18s ease;
          }
          .close-action-confirm-cancel:hover{background:#f8fafc;border-color:#cbd5e1;transform:translateY(-1px);}
          .close-action-confirm-confirm{
            padding:12px 18px;border:none;border-radius:14px;
            background:linear-gradient(135deg,#ef4444,#dc2626);color:#ffffff;
            font-size:14px;font-weight:800;cursor:pointer;
            box-shadow:0 12px 26px rgba(220,38,38,0.22);
            transition:transform 0.18s ease,box-shadow 0.18s ease;
          }
          .close-action-confirm-confirm:hover{transform:translateY(-1px);box-shadow:0 16px 30px rgba(220,38,38,0.28);}
          .close-action-confirm-cancel:focus-visible,
          .close-action-confirm-confirm:focus-visible{
            outline:none;box-shadow:0 0 0 4px rgba(59,130,246,0.15);
          }

          /* â”€â”€ AI Assistant â”€â”€ */
          .assistant-shell{
           position:fixed;
           right:90px;
           bottom:24px;
           z-index:10001;
           width:0;
           height:0;
           pointer-events:none;
          }
          .assistant-shell.open{pointer-events:auto;}
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
           position:absolute;
           right:0;
           bottom:0;
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
          .assistant-header-actions{display:flex;align-items:center;gap:8px;}
          .assistant-reset{border:1px solid #d7e2ee;background:white;color:#5b6b7b;border-radius:999px;padding:8px 12px;font-size:12px;font-weight:700;cursor:pointer;line-height:1;}
          .assistant-reset:hover{background:#f8fbff;color:#0f6cbd;}
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

          /* â”€â”€ Responsive â”€â”€ */
          @media(max-width:900px){
            .info-section{grid-template-columns:repeat(2,minmax(0,1fr));}
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
            .kpi-history-panel{width:calc(100% - 8px);}
            .kpi-card-actions{flex-direction:column;align-items:stretch;}
            .kpi-card-actions-right{justify-content:flex-end;}
            .history-filter-group{min-width:100%;flex-basis:100%;}
            .history-filter-clear{width:100%;justify-content:center;}
          }
          @media(max-width:600px){
            .info-section{grid-template-columns:1fr;}
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
            <div class="modal-icon" aria-hidden="true">&#9888;&#65039;</div>
            <h3>Confirm Submission</h3>
            <p>Are you sure you want to submit your KPI values?</p>
            <div class="modal-actions">
              <button id="cancelBtn" type="button" class="btn-cancel">Cancel</button>
              <button id="confirmBtn" type="button" class="btn-confirm">Yes, Submit</button>
            </div>
          </div>
        </div>

        <div id="closeActionConfirmModal" class="close-action-confirm-overlay" aria-hidden="true">
          <div class="close-action-confirm-card" role="dialog" aria-modal="true" aria-labelledby="closeActionConfirmTitle">
            <div class="close-action-confirm-hero">
              <div class="close-action-confirm-badge">
                <span aria-hidden="true">&#9888;&#65039;</span>
                Close Corrective Action
              </div>
              <h3 id="closeActionConfirmTitle" class="close-action-confirm-title">Are you sure you want to close this corrective action?</h3>
              <p id="closeActionConfirmMessage" class="close-action-confirm-text">
                Closing this corrective action will remove it from the current active list.
              </p>
            </div>
            <div class="close-action-confirm-body">
              <div id="closeActionConfirmNote" class="close-action-confirm-note">
                The corrective action will stay saved in the system, but it will no longer appear in the current list.
              </div>
              <div class="close-action-confirm-actions">
                <button id="closeActionConfirmCancel" type="button" class="close-action-confirm-cancel">Cancel</button>
                <button id="closeActionConfirmConfirm" type="button" class="close-action-confirm-confirm">Yes, close it</button>
              </div>
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

        <!-- â”€â”€ CA Table Modal â”€â”€ -->
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
                      <th>Due Date</th>
                      <th>Responsible</th>
                      <th>Status</th>
                      <th class="ca-col-actions">Actions</th>
                    </tr>
                  </thead>
                  <tbody id="caModalTableBody">
                    <tr><td colspan="7" class="ca-table-empty">No corrective actions yet.</td></tr>
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
                  <button type="button" class="ca-tbl-btn ca-tbl-delete" id="caModalFormCollapse">&times; Cancel</button>
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
                <div class="assistant-avatar" aria-hidden="true">&#129302;</div>
                <div>
                  <div class="assistant-title">AI Assistant</div>
                  <div class="assistant-focus" id="assistantFocus">All KPIs on this form</div>
                </div>
              </div>
              <div class="assistant-header-actions">
                <button type="button" class="assistant-reset" id="assistantReset">Reset</button>
                <button type="button" class="assistant-close" id="assistantClose" aria-label="Close">&times;</button>
              </div>
            </div>
            <div class="assistant-messages" id="assistantMessages"></div>
            <div class="assistant-composer">
              <div class="assistant-status" id="assistantStatus">Ask about KPI trends, quotation delays, root causes, owners, or corrective actions.</div>
              <form class="assistant-form" id="assistantForm">
                <textarea class="assistant-input" id="assistantInput" placeholder="Ask about KPIs or quote delays" rows="1"></textarea>
                <button type="submit" class="assistant-send" id="assistantSend" aria-label="Send">&#10148;</button>
              </form>
            </div>
          </div>
      
        </div>

        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script>
          /* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
             CA TABLE MODAL
          â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */
          let caModalKvId = null;
          // In-memory store: kvId â†’ array of action objects
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
      const status = getTrimmedValue(
        card.querySelector('input[name="ca_status_' + kvId + '[]"]')
      );

      if (rootCause || implSolution || dueDate || actionId) {
        caModalStore[kvId].push({
        id: actionId,
        root_cause: rootCause,
       implemented_solution: implSolution, //FIXED
       due_date: dueDate,
       responsible: responsible,
       status: getCanonicalCorrectiveActionStatus(status)
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

          const HISTORY_STATUS_OPTIONS = ["Open", "Waiting for validation", "Completed", "Closed"];

          function getCanonicalCorrectiveActionStatus(status) {
            const normalized = String(status || "").trim().toLowerCase();
            const matched = HISTORY_STATUS_OPTIONS.find(option => option.toLowerCase() === normalized);
            return matched || "Open";
          }

          function isClosedCorrectiveActionStatus(status) {
            return getCanonicalCorrectiveActionStatus(status) === "Closed";
          }

          let closeActionConfirmState = null;

          function settleCloseActionConfirm(confirmed) {
            if (!closeActionConfirmState) return;

            const pending = closeActionConfirmState;
            closeActionConfirmState = null;

            const modal = document.getElementById("closeActionConfirmModal");
            if (modal) {
              modal.classList.remove("active");
              modal.setAttribute("aria-hidden", "true");
            }

            if (!confirmed && pending.triggerEl && document.contains(pending.triggerEl)) {
              window.setTimeout(() => pending.triggerEl.focus(), 0);
            }

            pending.resolve(Boolean(confirmed));
          }

          function confirmCloseCorrectiveAction(options) {
            const modal = document.getElementById("closeActionConfirmModal");
            const message = document.getElementById("closeActionConfirmMessage");
            const note = document.getElementById("closeActionConfirmNote");
            const confirmButton = document.getElementById("closeActionConfirmConfirm");

            if (!modal) {
              return Promise.resolve(window.confirm("Are you sure you want to close this corrective action?"));
            }

            if (message) {
              message.textContent = "Closing this corrective action will remove it from the current active list.";
            }

            if (note) {
              note.textContent = "The corrective action will stay saved in the system, but it will no longer appear in the current list.";
            }

            if (closeActionConfirmState) {
              settleCloseActionConfirm(false);
            }

            return new Promise((resolve) => {
              closeActionConfirmState = {
                resolve,
                triggerEl: options && options.triggerEl ? options.triggerEl : null
              };

              modal.classList.add("active");
              modal.setAttribute("aria-hidden", "false");

              if (confirmButton) {
                window.setTimeout(() => confirmButton.focus(), 0);
              }
            });
          }

          function setCorrectiveActionBadgeState(badge, status) {
            if (!badge) return;
            const canonicalStatus = getCanonicalCorrectiveActionStatus(status);
            badge.textContent = canonicalStatus;
            badge.className = "ca-status-badge ca-status-" + statusClass(canonicalStatus);
          }

          function updateCorrectiveActionStatusInStore(kvId, actionIndex, status) {
            const actions = getCaModalActions(kvId);
            if (!actions[actionIndex]) return null;

            const canonicalStatus = getCanonicalCorrectiveActionStatus(status);
            actions[actionIndex].status = canonicalStatus;
            syncDomFromStore(kvId);

            if (caModalKvId === kvId) {
              renderCaModalTable(kvId);
            }

            return canonicalStatus;
          }

          function syncHistoryActionStatusInCard(kvId, actionId, status) {
            const normalizedActionId = String(actionId || "").trim();
            if (!normalizedActionId) return;

            const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');
            if (!card) return;

            const historyActions = decodeModalPayload(card.dataset.historyActions, []);
            if (!Array.isArray(historyActions) || !historyActions.length) return;

            let hasChanges = false;
            const canonicalStatus = getCanonicalCorrectiveActionStatus(status);
            const nextHistoryActions = historyActions.map(function(action) {
              const currentActionId = String(
                (action && (action.id || action.corrective_action_id)) || ""
              ).trim();

              if (currentActionId !== normalizedActionId) {
                return action;
              }

              hasChanges = true;
              return Object.assign({}, action, { status: canonicalStatus });
            });

            if (hasChanges) {
              card.dataset.historyActions = encodeModalPayload(nextHistoryActions);
            }
          }

          function truncate(str, n) {
            const s = String(str || "");
            return s.length > n ? s.slice(0, n) + "â€¦" : s;
          }

        function renderCaModalTable(kvId) {
       const tbody = document.getElementById("caModalTableBody");
       if (!tbody) return;
       const actions = getCaModalActions(kvId);
       if (!actions.length) {
       tbody.innerHTML = '<tr><td colspan="7" class="ca-table-empty">No corrective actions yet. Click "Add" below to get started.</td></tr>';
       return;
       }

       tbody.innerHTML = actions.map((a, i) => {
      const sc = statusClass(a.status);
      return \`<tr>
      <td class="ca-col-num">\${i + 1}</td>
      <td title="\${escapeHtml(a.root_cause)}">\${escapeHtml(truncate(a.root_cause, 60))}</td>
      <td title="\${escapeHtml(a.implemented_solution)}">\${escapeHtml(truncate(a.implemented_solution, 60))}</td>
      <td>\${escapeHtml(a.due_date)}</td>
      <td>\${escapeHtml(a.responsible)}</td>
      <td>\${a.status ? \`<span class="ca-table-status \${sc}">\${escapeHtml(a.status)}</span>\` : "&mdash;"}</td>
      <td class="ca-col-actions">
        <button type="button" class="ca-table-edit-btn" onclick="caModalOpenForm(\${i})">Edit</button>
        <button type="button" class="ca-table-delete-btn" onclick="caModalDeleteAction(\${i})">&times;</button>
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
  const existingAction =
    editIndex !== "" && !isNaN(parseInt(editIndex, 10))
      ? actions[parseInt(editIndex, 10)] || null
      : null;

  const entry = {
   id: "",
   root_cause: rootCause,
    implemented_solution: solution, // FIXED
    due_date: dueDate,
    responsible: responsible,
    status: getCanonicalCorrectiveActionStatus(existingAction && existingAction.status)
  };

  if (editIndex !== "" && !isNaN(parseInt(editIndex, 10))) {
    const idx = parseInt(editIndex, 10);
    if (actions[idx] && actions[idx].id) {
      entry.id = actions[idx].id;
    }
    actions.splice(idx, 1);
    actions.unshift(entry);
  } else {
    actions.unshift(entry);
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
    if (statusInput) statusInput.value = getCanonicalCorrectiveActionStatus(action.status);

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
      setCorrectiveActionBadgeState(badge, action.status);
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
            if (!overlay) return;
            const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');
            const title = card ? (card.querySelector(".kpi-title") || {}).textContent || "KPI" : "KPI";

            const modalTitle = document.getElementById("caModalTitle");
            const modalSubtitle = document.getElementById("caModalSubtitle");
            if (modalTitle) modalTitle.textContent = "Corrective Actions â€” " + title.trim();
            if (modalSubtitle) modalSubtitle.textContent = "Add, edit, or remove corrective action entries";

            caModalCollapseForm();
            renderCaModalTable(kvId);

            overlay.classList.add("active");
            overlay.setAttribute("aria-hidden", "false");
            document.body.classList.add("chart-modal-open");
          }

          function closeCaTableModal() {
            const overlay = document.getElementById("caTableModal");
            if (!overlay) return;
            overlay.classList.remove("active");
            overlay.setAttribute("aria-hidden", "true");
            if (!document.querySelector(".chart-modal-overlay.active") &&
                !document.querySelector(".history-modal-overlay.active")) {
              document.body.classList.remove("chart-modal-open");
            }
            caModalKvId = null;
          }

          /* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
             UTILITY HELPERS
          â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */
          function getTextContent(node) { return node && typeof node.textContent === "string" ? node.textContent : ""; }
          function getTrimmedText(node) { return getTextContent(node).trim(); }
          function getInputValue(node) { return node && typeof node.value === "string" ? node.value : ""; }
          function getTrimmedValue(node) { return getInputValue(node).trim(); }
          function normalizeMetricInputValue(value) {
            const text = String(value ?? "").trim();
            if (!text) return "";
            const numeric = Number(text);
            return Number.isFinite(numeric) ? String(numeric) : text;
          }
          function setValueInputServerState(input, serverValue) {
            if (!input) return;
            input.dataset.serverValue = normalizeMetricInputValue(serverValue);
            input.dataset.dirty = "false";
          }
          function syncValueInputDirtyState(input) {
            if (!input) return false;
            const currentValue = normalizeMetricInputValue(input.value);
            const serverValue = normalizeMetricInputValue(input.dataset.serverValue);
            const isDirty = currentValue !== serverValue;
            input.dataset.dirty = isDirty ? "true" : "false";
            return isDirty;
          }

          function getCorrectiveActionStack(kvId) {
            return document.querySelector('.ca-actions-stack[data-kpi-values-id="' + kvId + '"]');
          }
          function getCorrectiveActionCards(kvId) {
            const stack = getCorrectiveActionStack(kvId);
            return stack ? Array.from(stack.querySelectorAll(".ca-action-card")) : [];
          }
          function getLatestCorrectiveActionCard(kvId) {
            const cards = getCorrectiveActionCards(kvId);
            return cards.length ? cards[0] : null;
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
              const latestAction = correctiveActions.length ? correctiveActions[0] : {};
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

          /* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
             AI ASSISTANT
          â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */
          const assistantShell = document.getElementById("assistantShell");
          const assistantLauncher = document.getElementById("assistantLauncher");
          const assistantClose = document.getElementById("assistantClose");
          const assistantMessages = document.getElementById("assistantMessages");
          const assistantFocus = document.getElementById("assistantFocus");
          const assistantStatus = document.getElementById("assistantStatus");
          const assistantForm = document.getElementById("assistantForm");
          const assistantInput = document.getElementById("assistantInput");
          const assistantSend = document.getElementById("assistantSend");
          const assistantReset = document.getElementById("assistantReset");
          const assistantStorageKey = "kpi-assistant:${responsible_id}:${week}";
          const assistantState = {
            selectedKpiId: null,
            pending: false,
            threads: loadAssistantThreads()
          };

          function normalizeAssistantThreadMessage(entry) {
            const role = entry && (entry.role === "assistant" || entry.role === "user") ? entry.role : null;
            const text = String(entry?.text ?? "").trim();
            const contextText = String(entry?.contextText ?? entry?.content ?? text).trim();
            if (!role || !text || !contextText) return null;
            return {
              role,
              text,
              contextText,
              ts: Number(entry?.ts) || Date.now()
            };
          }

          function loadAssistantThreads() {
            try {
              const raw = window.localStorage ? window.localStorage.getItem(assistantStorageKey) : null;
              if (!raw) return {};
              const parsed = JSON.parse(raw);
              const normalized = {};
              Object.keys(parsed || {}).forEach((key) => {
                const threadMessages = Array.isArray(parsed[key]?.messages)
                  ? parsed[key].messages.map(normalizeAssistantThreadMessage).filter(Boolean).slice(-18)
                  : [];
                normalized[key] = { messages: threadMessages };
              });
              return normalized;
            } catch (err) {
              return {};
            }
          }

          function persistAssistantThreads() {
            try {
              if (!window.localStorage) return;
              window.localStorage.setItem(assistantStorageKey, JSON.stringify(assistantState.threads));
            } catch (err) {
              // Ignore storage errors so the assistant remains usable.
            }
          }

          function clearAssistantThreads() {
            assistantState.threads = {};
            try {
              if (window.localStorage) {
                window.localStorage.removeItem(assistantStorageKey);
              }
            } catch (err) {
              // Ignore storage errors so reset still clears in-memory state.
            }
          }

          function getAssistantThreadKey(kvId) {
            const resolvedKpiId = kvId === undefined ? assistantState.selectedKpiId : kvId;
            return resolvedKpiId === null || resolvedKpiId === undefined || resolvedKpiId === ""
              ? "all"
              : "kpi:" + String(resolvedKpiId);
          }

          function ensureAssistantThread(threadKey) {
            const key = threadKey || getAssistantThreadKey();
            if (!assistantState.threads[key] || !Array.isArray(assistantState.threads[key].messages)) {
              assistantState.threads[key] = { messages: [] };
            }
            return assistantState.threads[key];
          }

          function getCurrentAssistantThread() {
            return ensureAssistantThread();
          }

          function pushAssistantThreadMessage(role, text, contextText) {
            const cleanText = String(text || "").trim();
            const cleanContextText = String(contextText || text || "").trim();
            if (!cleanText || !cleanContextText) return;
            const thread = getCurrentAssistantThread();
            thread.messages.push({
              role,
              text: cleanText,
              contextText: cleanContextText,
              ts: Date.now()
            });
            if (thread.messages.length > 18) {
              thread.messages = thread.messages.slice(-18);
            }
            persistAssistantThreads();
          }

          function getAssistantRequestHistory(latestContextText) {
            const thread = getCurrentAssistantThread();
            const history = thread.messages.map((entry) => ({
              role: entry.role,
              content: entry.contextText || entry.text
            }));
            const last = history[history.length - 1];
            if (
              latestContextText &&
              last &&
              last.role === "user" &&
              String(last.content || "").trim() === String(latestContextText || "").trim()
            ) {
              history.pop();
            }
            return history.slice(-14);
          }

          function appendAssistantMessageInstant(role, text) {
            if (!assistantMessages) return;
            const msg = document.createElement("div");
            msg.className = "assistant-message " + role;
            msg.innerHTML = String(text || "")
              .replace(/&/g, "&amp;")
              .replace(/</g, "&lt;")
              .replace(/>/g, "&gt;")
              .split("\\n")
              .join("<br>");
            assistantMessages.appendChild(msg);
            assistantMessages.scrollTop = assistantMessages.scrollHeight;
          }

          function addAssistantMessage(role, text) {
          return new Promise((resolve) => {
        if (!assistantMessages) {
         resolve();
         return;
         }

    const msg = document.createElement("div");
    msg.className = "assistant-message " + role;
    assistantMessages.appendChild(msg);
    assistantMessages.scrollTop = assistantMessages.scrollHeight;

    const safeText = String(text || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");

    if (role !== "assistant" || safeText.length > 400) {
      msg.innerHTML = safeText.split("\\n").join("<br>");
      assistantMessages.scrollTop = assistantMessages.scrollHeight;
      resolve();
      return;
    }

    let i = 0;
    const typingSpeed = 5;

    function typeNext() {
      const partial = safeText.slice(0, i).split("\\n").join("<br>");
      msg.innerHTML = partial + '<span class="assistant-cursor">|</span>';
      assistantMessages.scrollTop = assistantMessages.scrollHeight;

      if (i < safeText.length) {
        i++;
        setTimeout(typeNext, typingSpeed);
      } else {
        msg.innerHTML = safeText.split("\\n").join("<br>");
        assistantMessages.scrollTop = assistantMessages.scrollHeight;
        resolve();
      }
    }

    typeNext();
  });
}
          function setAssistantStatus(text) { if (assistantStatus) assistantStatus.textContent = text; }
          function getKpiCardById(kvId) { return document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]'); }
          function getAssistantKpiDisplayName(card) {
            if (!card) return "Selected KPI";
            const title = getTrimmedText(card.querySelector(".kpi-title"));
            const subtitle = getTrimmedText(card.querySelector(".kpi-subtitle"));
            return subtitle && title ? (subtitle + " (" + title + ")") : (subtitle || title || "Selected KPI");
          }

          function buildAssistantGreeting() {
          if (!assistantState.selectedKpiId) {
           return "Hello. Describe the issue you want to diagnose. I will help identify the root cause, action, owner, and deadline using the available knowledge base.";
           }

           const card = getKpiCardById(assistantState.selectedKpiId);
           const kpiName = getAssistantKpiDisplayName(card);

           return "Focused on: " + kpiName + "\\n\\nDescribe the issue, delay, or risk you want to diagnose.";
          } 

          function ensureAssistantGreeting() {
            const thread = getCurrentAssistantThread();
            if (!thread.messages.length) {
              const greeting = buildAssistantGreeting();
              pushAssistantThreadMessage("assistant", greeting, greeting);
            }
          }

          function renderAssistantConversation() {
            if (!assistantMessages) return;
            ensureAssistantGreeting();
            assistantMessages.innerHTML = "";
            getCurrentAssistantThread().messages.forEach((entry) => {
              appendAssistantMessageInstant(entry.role, entry.text);
            });
          }

          function extractGuidedQuestionOptions(text) {
            const lines = String(text || "")
              .replaceAll("\\r", "")
              .split("\\n")
              .map((line) => line.trim())
              .filter(Boolean);
            const options = {};

            lines.forEach((line) => {
              const separatorIndex = line.indexOf(". ");
              if (separatorIndex <= 0) return;
              const optionNumber = line.slice(0, separatorIndex).trim();
              const optionText = line.slice(separatorIndex + 2).trim();
              if (optionNumber.length === 1 && optionNumber >= "1" && optionNumber <= "9" && optionText) {
                options[optionNumber] = optionText;
              }
            });

            if (!Object.keys(options).length) return null;

            const questionLine = lines.find((line) => {
              const separatorIndex = line.indexOf(". ");
              if (separatorIndex <= 0) return true;
              const optionNumber = line.slice(0, separatorIndex).trim();
              return !(optionNumber.length === 1 && optionNumber >= "1" && optionNumber <= "9");
            }) || "Previous guided question";
            return {
              question: questionLine.endsWith(":")
                ? questionLine.slice(0, -1).trim()
                : questionLine,
              options
            };
          }

          function resolveAssistantUserMessage(rawMessage) {
            const text = String(rawMessage || "").trim();
            if (!text) return "";

            const lastAssistantMessage = [...getCurrentAssistantThread().messages]
              .reverse()
              .find((entry) => entry.role === "assistant");
            const guidedQuestion = extractGuidedQuestionOptions(lastAssistantMessage?.text || "");
            if (!guidedQuestion) return text;

            const tokens = text.split(" ").filter(Boolean);
            if (!tokens.length) return text;

            const startsWithOptionWord = tokens[0].toLowerCase() === "option";
            const rawOptionToken = startsWithOptionWord ? tokens[1] : tokens[0];
            if (!rawOptionToken) return text;

            let optionNumber = rawOptionToken.trim();
            if (optionNumber.endsWith(".") || optionNumber.endsWith(")")) {
              optionNumber = optionNumber.slice(0, -1);
            }
            if (!(optionNumber.length === 1 && optionNumber >= "1" && optionNumber <= "9")) {
              return text;
            }

            const optionLabel = guidedQuestion.options[optionNumber];
            if (!optionLabel) return text;

            const additionalDetail = (startsWithOptionWord ? tokens.slice(2) : tokens.slice(1)).join(" ").trim();
            return additionalDetail
              ? 'Answer to the previous question "' + guidedQuestion.question + '": ' + optionNumber + '. ' + optionLabel + '. Additional detail: ' + additionalDetail
              : 'Answer to the previous question "' + guidedQuestion.question + '": ' + optionNumber + '. ' + optionLabel;
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
            if (assistantLauncher) assistantLauncher.innerHTML = "&times;";
            if (assistantFocus) {
              if (!assistantState.selectedKpiId) {
                assistantFocus.textContent = "All KPIs on this form";
              } else {
                const card = getKpiCardById(assistantState.selectedKpiId);
                assistantFocus.textContent = "Focused on: " + getAssistantKpiDisplayName(card);
              }
            }
            syncAssistantInputPlaceholder();
            renderAssistantConversation();
            if (assistantInput) assistantInput.focus();
          }
          function closeAssistant() {
            if (!assistantShell) return;
            assistantShell.classList.remove("open");
            if (assistantLauncher) assistantLauncher.innerHTML = "&#129302;";
          }

          function resetAssistantForTesting() {
            clearAssistantThreads();
            renderAssistantConversation();
            setAssistantStatus("Assistant reset. Start the workflow again.");
            if (assistantInput) assistantInput.focus();
          }
          window.resetAssistantForTesting = resetAssistantForTesting;

          function openAssistantForKpi(kvId) {
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

  const resolvedMessage = resolveAssistantUserMessage(cleanMessage);

  assistantState.pending = true;
  if (assistantSend) assistantSend.disabled = true;

  pushAssistantThreadMessage("user", cleanMessage, resolvedMessage);
  await addAssistantMessage("user", cleanMessage);
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
        message: resolvedMessage,
        conversation_history: getAssistantRequestHistory(resolvedMessage)
      })
    });

    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "Request failed");

    const assistantReply = data.reply || "I could not generate a response.";
    pushAssistantThreadMessage("assistant", assistantReply, assistantReply);

    await addAssistantMessage(
      "assistant",
      assistantReply
    );

    setAssistantStatus("AI assistant is ready.");
  } catch (err) {
    const fallbackReply = "I could not answer right now. Please try again.";
    pushAssistantThreadMessage("assistant", fallbackReply, fallbackReply);
    await addAssistantMessage(
      "assistant",
      fallbackReply
    );
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
            assistantState.selectedKpiId = null;
            openAssistant();
          });
          if (assistantClose) assistantClose.addEventListener("click", closeAssistant);
          if (assistantReset) assistantReset.addEventListener("click", resetAssistantForTesting);
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

          /* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
             CHARTS
          â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */
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
    "&kpi_values_id=" + encodeURIComponent(kvId) +
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
  const preserveDraftValue = input
    ? (syncValueInputDirtyState(input) || document.activeElement === input)
    : false;

  if (input && !preserveDraftValue) {
    const nextValue = data.currentValue !== null && data.currentValue !== undefined
      ? String(data.currentValue)
      : "";
    input.value = nextValue;
    setValueInputServerState(input, nextValue);
  }

  const chart = kpiCharts[kvId];
  if (!chart) {
    buildKpiChart(kvId);
    if (preserveDraftValue && input) {
      updateCurrentMonthBarFromInput(kvId, input.value);
    }
    return;
  }

  chart.data.labels = data.labels;
  chart.data.datasets[0].data = data.values;
  if (preserveDraftValue && input) {
    updateCurrentMonthBarFromInput(kvId, input.value);
    return;
  }

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

          const kpiBarFlowLinePlugin = {
            id: "kpiBarFlowLine",
            afterDatasetsDraw(chart, args, opts) {
              const datasetIndex = Number.isInteger(opts?.datasetIndex) ? opts.datasetIndex : 0;
              const meta = chart.getDatasetMeta(datasetIndex);
              const area = chart.chartArea;
              if (!meta?.data?.length || !area) return;

              const offset = Number.isFinite(opts?.offset) ? opts.offset : 12;
              const pointRadius = Number.isFinite(opts?.pointRadius) ? opts.pointRadius : 3.5;
              const lineWidth = Number.isFinite(opts?.lineWidth) ? opts.lineWidth : 3;
              const color = opts?.color || "#111827";
              const pointFill = opts?.pointFill || color;
              const pointStroke = opts?.pointStroke || "#ffffff";
              const points = [];

              meta.data.forEach((barElement, index) => {
                const rawValue = chart.data?.datasets?.[datasetIndex]?.data?.[index];
                const numericValue = Number(rawValue);

                if (!Number.isFinite(numericValue) || !Number.isFinite(barElement?.x) || !Number.isFinite(barElement?.y)) {
                  return;
                }

                points.push({
                  x: barElement.x,
                  y: Math.max(barElement.y - offset, area.top + 8)
                });
              });

              if (points.length < 2) return;

              const ctx = chart.ctx;
              ctx.save();
              ctx.lineWidth = lineWidth;
              ctx.strokeStyle = color;
              ctx.lineCap = "round";
              ctx.lineJoin = "round";
              ctx.beginPath();
              ctx.moveTo(points[0].x, points[0].y);

              for (let index = 1; index < points.length; index += 1) {
                const previousPoint = points[index - 1];
                const currentPoint = points[index];
                const cp1x = previousPoint.x + (currentPoint.x - previousPoint.x) * 0.35;
                const cp1y = previousPoint.y;
                const cp2x = currentPoint.x - (currentPoint.x - previousPoint.x) * 0.35;
                const cp2y = currentPoint.y;
                ctx.bezierCurveTo(cp1x, cp1y, cp2x, cp2y, currentPoint.x, currentPoint.y);
              }

              ctx.stroke();

              points.forEach((point) => {
                ctx.beginPath();
                ctx.fillStyle = pointFill;
                ctx.arc(point.x, point.y, pointRadius, 0, Math.PI * 2);
                ctx.fill();
                ctx.lineWidth = 1.5;
                ctx.strokeStyle = pointStroke;
                ctx.stroke();
                ctx.lineWidth = lineWidth;
                ctx.strokeStyle = color;
              });

              ctx.restore();
            }
          };

          function getThresholdLines(lowLimit, target, highLimit) {
            const lines = [];
            const low = parseFloat(lowLimit);
            const targetValue = parseFloat(target);
            const high = parseFloat(highLimit);
            const hasLow = !isNaN(low);
            const hasTarget = !isNaN(targetValue);
            const hasHigh = !isNaN(high);

            if (hasHigh) {
              lines.push({
                key:"high_limit",
                label:"Upper Limit",
                value:high,
                borderColor:"#f59e0b",
                borderDash:[8,5],
                borderWidth:2
              });
            }

            if (hasTarget) {
              lines.push({
                key:"target",
                label:"Target",
                value:targetValue,
                borderColor:"#22c55e",
                borderDash:[8,5],
                borderWidth:2
              });
            }

            if (hasLow) {
              lines.push({
                key:"low_limit",
                label:"Lower Limit",
                value:low,
                borderColor:"#ef4444",
                borderDash:[8,5],
                borderWidth:2
              });
            }

            return lines;
          }

          function getPointColor(value, lowLimit, highLimit) {
            const val = parseFloat(value);
            const rawLow = parseFloat(lowLimit);
            const rawHigh = parseFloat(highLimit);
            const hasLow = !isNaN(rawLow);
            const hasHigh = !isNaN(rawHigh);
            const lower = hasLow && hasHigh ? Math.min(rawLow, rawHigh) : hasLow ? rawLow : null;
            const upper = hasLow && hasHigh ? Math.max(rawLow, rawHigh) : hasHigh ? rawHigh : null;

            if (isNaN(val)) return "#94a3b8";
            if (upper !== null && val > upper) return "#e34b24";
            if (lower !== null && val < lower) return "#2f6b22";
            return "#b7dc58";
          }

          function getPointBorderColor(value, lowLimit, highLimit) {
            const fill = getPointColor(value, lowLimit, highLimit);
            if (fill === "#e34b24") return "#ba3415";
            if (fill === "#2f6b22") return "#1f4d17";
            if (fill === "#b7dc58") return "#86a93a";
            return "#64748b";
          }

          function computeBounds(values, lowLimit, target, highLimit) {
            const all = (Array.isArray(values) ? values : [])
              .filter(v => v !== null && v !== undefined && v !== "")
              .map(v => Number(v))
              .filter(v => Number.isFinite(v));
            if (!isNaN(lowLimit)) all.push(lowLimit);
            if (!isNaN(target)) all.push(target);
            if (!isNaN(highLimit)) all.push(highLimit);
            if (!all.length) return { min: 0, max: 100 };

            const sourceMin = Math.min(...all);
            const sourceMax = Math.max(...all);
            let min = sourceMin > 0
              ? sourceMin * 0.8
              : sourceMin < 0
                ? sourceMin * 1.2
                : 0;
            let max = sourceMax > 0
              ? sourceMax * 1.2
              : sourceMax < 0
                ? sourceMax * 0.8
                : 0;

            if (min === max) {
              const pad = Math.max(Math.abs(sourceMax || sourceMin || 1) * 0.2, 1);
              min = sourceMin - pad;
              max = sourceMax + pad;
            }

            return { min, max };
          }

          function buildKpiChart(kvId) {
            const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');
            if (!card) return;
            const canvas = document.getElementById("chart_" + kvId);
            if (!canvas) return;
            const lowLimit = parseFloat(card.dataset.lowLimit);
            const highLimit = parseFloat(card.dataset.highLimit);
            const target = parseFloat(card.dataset.target);
            let labels = [], values = [];
            try { labels = JSON.parse(card.dataset.historyLabels || "[]"); values = JSON.parse(card.dataset.historyValues || "[]"); } catch(e){}
            const colors = values.map(v => getPointColor(v, lowLimit, highLimit));
            const borderColors = values.map(v => getPointBorderColor(v, lowLimit, highLimit));
            const bounds = computeBounds(values, lowLimit, target, highLimit);
            kpiCharts[kvId] = new Chart(canvas.getContext("2d"), {
              type:"bar",
              data:{ labels, datasets:[{ label:"Value", data:values, backgroundColor:colors, borderColor:borderColors, borderWidth:1.5, borderRadius:6, borderSkipped:false, barThickness:30 }] },
              options:{
                responsive:true, maintainAspectRatio:false, animation:false,
                layout:{ padding:{ right:12 } },
                plugins:{
                  legend:{ display:false },
                  kpiThresholdLines:{ lines: getThresholdLines(lowLimit,target,highLimit) },
                  kpiBarFlowLine:{ datasetIndex:0, offset:12, lineWidth:3, pointRadius:3.5 }
                },
                scales:{ x:{ grid:{ display:false } }, y:{ min:bounds.min, max:bounds.max } }
              },
              plugins:[kpiThresholdLinesPlugin, kpiBarFlowLinePlugin]
            });
          }

          function updateKpiChart(kvId) {
            const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');
            if (!card) return;
            const chart = kpiCharts[kvId]; if (!chart) return;
            const lowLimit = parseFloat(card.dataset.lowLimit), highLimit = parseFloat(card.dataset.highLimit), target = parseFloat(card.dataset.target);
            const data = chart.data.datasets[0].data;
            const colors = data.map(v => getPointColor(v, lowLimit, highLimit));
            const borderColors = data.map(v => getPointBorderColor(v, lowLimit, highLimit));
            chart.data.datasets[0].backgroundColor = colors;
            chart.data.datasets[0].borderColor = borderColors;
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
              c.borderColor = Array.isArray(d.borderColor) ? d.borderColor.slice() : d.borderColor;
              if (i===0){ c.barThickness=56; c.maxBarThickness=72; c.borderRadius=10; }
              return c;
            });
            expandedChart.options.scales.y.min = src.options.scales.y.min;
            expandedChart.options.scales.y.max = src.options.scales.y.max;
            expandedChart.options.plugins.kpiThresholdLines.lines = (src.options.plugins.kpiThresholdLines.lines||[]).map(l=>({...l,borderDash:(l.borderDash||[]).slice()}));
            expandedChart.options.plugins.kpiBarFlowLine = Object.assign({}, src.options.plugins.kpiBarFlowLine || { datasetIndex:0, offset:14, lineWidth:3, pointRadius:4 });
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
                  plugins:{
                    legend:{display:false},
                    kpiThresholdLines:{ lines:(src.options.plugins.kpiThresholdLines.lines||[]).map(l=>({...l,borderDash:(l.borderDash||[]).slice()})) },
                    kpiBarFlowLine: Object.assign({}, src.options.plugins.kpiBarFlowLine || { datasetIndex:0, offset:14, lineWidth:3, pointRadius:4 })
                  },
                  scales:{ x:{grid:{display:false},ticks:{color:"#475569",font:{size:14,weight:"600"}}}, y:{min:src.options.scales.y.min,max:src.options.scales.y.max,ticks:{color:"#64748b",font:{size:13}},grid:{color:"#e2e8f0"}} }
                },
                plugins:[expandedChartValueLabelsPlugin, kpiThresholdLinesPlugin, kpiBarFlowLinePlugin]
              });
            });
          }

          /* â”€â”€ History modal helpers â”€â”€ */
          function escapeHistoryHtml(v) { return String(v||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#39;"); }
          function formatHistoryText(v) { return escapeHistoryHtml(v).split("\\n").join("<br>"); }
          function encodeModalPayload(v) { try { return encodeURIComponent(JSON.stringify(v ?? null)); } catch(e){ return ""; } }
          function decodeModalPayload(v, fb) { try { const d=decodeURIComponent(v||""); if(!d) return fb; const p=JSON.parse(d); return (p===null||p===undefined)?fb:p; } catch(e){ return fb; } }
          function formatHistoryWeek(w) { const m=String(w||"").match(/^(\\d{4})-Week(\\d{1,2})$/); return m?"Week "+parseInt(m[2],10):escapeHistoryHtml(w||""); }
          function getHistoryStatusClass(s) { return "status-"+String(s||"Open").trim().toLowerCase().replace(/\s+/g,"-"); }
          function getHistoryDueDateFilterValue(rawDueDate) {
            const text = String(rawDueDate || "").trim();
            if (!text) return "";

            const isoMatch = text.match(/^(\d{4}-\d{2}-\d{2})/);
            if (isoMatch) {
              return isoMatch[1];
            }

            const parsedDate = new Date(text);
            if (isNaN(parsedDate.getTime())) {
              return "";
            }

            const year = String(parsedDate.getFullYear());
            const month = String(parsedDate.getMonth() + 1).padStart(2, "0");
            const day = String(parsedDate.getDate()).padStart(2, "0");
            return year + "-" + month + "-" + day;
          }

          function applyHistoryDueDateFilters(panelEl) {
            if (!panelEl) return;

            const fromInput = panelEl.querySelector('[data-history-filter="due-from"]');
            const toInput = panelEl.querySelector('[data-history-filter="due-to"]');
            const clearButton = panelEl.querySelector('[data-history-filter="clear"]');
            const tableWrap = panelEl.querySelector("[data-history-table-wrap]");
            const emptyState = panelEl.querySelector("[data-history-filter-empty]");
            const rows = panelEl.querySelectorAll("tbody tr");

            const fromValue = String(fromInput?.value || "").trim();
            const toValue = String(toInput?.value || "").trim();
            let visibleCount = 0;

            rows.forEach(function(row) {
              const dueDateValue = String(row.dataset.historyDueDate || "").trim();
              let isVisible = true;

              if (fromValue) {
                isVisible = Boolean(dueDateValue) && dueDateValue >= fromValue;
              }

              if (isVisible && toValue) {
                isVisible = Boolean(dueDateValue) && dueDateValue <= toValue;
              }

              row.hidden = !isVisible;

              if (isVisible) {
                visibleCount += 1;
                const numberCell = row.querySelector(".history-row-number");
                if (numberCell) {
                  numberCell.textContent = String(visibleCount);
                }
              }
            });

            if (tableWrap) {
              tableWrap.hidden = visibleCount === 0;
            }

            if (emptyState) {
              emptyState.hidden = visibleCount !== 0;
            }

            if (clearButton) {
              clearButton.disabled = !(fromValue || toValue);
            }
          }

          function bindHistoryDueDateFilters() {
            document.querySelectorAll("[data-history-filter-panel]").forEach(function(panelEl) {
              const fromInput = panelEl.querySelector('[data-history-filter="due-from"]');
              const toInput = panelEl.querySelector('[data-history-filter="due-to"]');
              const clearButton = panelEl.querySelector('[data-history-filter="clear"]');

              if (!fromInput || !toInput) {
                return;
              }

              const syncInputsAndApply = function(source) {
                if (fromInput.value && toInput.value && fromInput.value > toInput.value) {
                  if (source === "from") {
                    toInput.value = fromInput.value;
                  } else if (source === "to") {
                    fromInput.value = toInput.value;
                  }
                }

                if (toInput.value) {
                  fromInput.max = toInput.value;
                } else {
                  fromInput.removeAttribute("max");
                }

                if (fromInput.value) {
                  toInput.min = fromInput.value;
                } else {
                  toInput.removeAttribute("min");
                }

                applyHistoryDueDateFilters(panelEl);
              };

              fromInput.oninput = function() {
                syncInputsAndApply("from");
              };

              toInput.oninput = function() {
                syncInputsAndApply("to");
              };

              if (clearButton) {
                clearButton.onclick = function() {
                  fromInput.value = "";
                  toInput.value = "";
                  syncInputsAndApply();
                };
              }

              syncInputsAndApply();
            });
          }

          function renderHistoryStatusContent(action, options) {
            const canonicalStatus = getCanonicalCorrectiveActionStatus(action.status);
            const allowEditableStatus = Boolean(
              options &&
              options.editableStatus &&
              action &&
              action._editableStatus
            );

            if (!allowEditableStatus) {
              return canonicalStatus
                ? '<span class="history-chip ' + getHistoryStatusClass(canonicalStatus) + '">' + escapeHistoryHtml(canonicalStatus) + '</span>'
                : '&mdash;';
            }

            const actionIndex = Number.isInteger(action._storeIndex) ? String(action._storeIndex) : "";
            const actionId = String(action.id || action.corrective_action_id || "").trim();

            return '' +
              '<select class="history-status-select ' + getHistoryStatusClass(canonicalStatus) + '"' +
                ' data-kpi-values-id="' + escapeHistoryHtml(options.kvId || "") + '"' +
                ' data-action-index="' + escapeHistoryHtml(actionIndex) + '"' +
                ' data-action-id="' + escapeHistoryHtml(actionId) + '"' +
                ' data-previous-status="' + escapeHistoryHtml(canonicalStatus) + '"' +
              '>' +
                HISTORY_STATUS_OPTIONS.map(function(option) {
                  return '<option value="' + escapeHistoryHtml(option) + '"' +
                    (option === canonicalStatus ? ' selected' : '') +
                    '>' + escapeHistoryHtml(option) + '</option>';
                }).join("") +
              '</select>';
          }

          function applyHistoryStatusSelectState(selectEl, status) {
            if (!selectEl) return;
            const canonicalStatus = getCanonicalCorrectiveActionStatus(status);
            selectEl.value = canonicalStatus;
            selectEl.className = "history-status-select " + getHistoryStatusClass(canonicalStatus);
          }

          async function handleHistoryStatusChange(selectEl) {
            if (!selectEl) return;

            const kvId = String(selectEl.dataset.kpiValuesId || "").trim();
            const actionIndex = parseInt(selectEl.dataset.actionIndex || "", 10);
            const hasActionIndex = !Number.isNaN(actionIndex);
            const actionId = String(selectEl.dataset.actionId || "").trim();

            if (!kvId || (!hasActionIndex && !actionId)) {
              applyHistoryStatusSelectState(selectEl, selectEl.dataset.previousStatus || "Open");
              return;
            }

            const previousStatus = getCanonicalCorrectiveActionStatus(selectEl.dataset.previousStatus || "Open");
            const nextStatus = getCanonicalCorrectiveActionStatus(selectEl.value);

            if (
              isClosedCorrectiveActionStatus(nextStatus) &&
              !isClosedCorrectiveActionStatus(previousStatus)
            ) {
              const confirmed = await confirmCloseCorrectiveAction({ triggerEl: selectEl });
              if (!confirmed) {
                applyHistoryStatusSelectState(selectEl, previousStatus);
                return;
              }
            }

            if (!actionId) {
              if (isClosedCorrectiveActionStatus(nextStatus)) {
                removeCorrectiveActionFromStore(kvId, actionIndex);
                selectEl.dataset.previousStatus = nextStatus;
                openHistoryModal(kvId);
                return;
              }

              applyHistoryStatusSelectState(selectEl, nextStatus);
              updateCorrectiveActionStatusInStore(kvId, actionIndex, nextStatus);
              selectEl.dataset.previousStatus = nextStatus;
              return;
            }

            selectEl.disabled = true;
            applyHistoryStatusSelectState(selectEl, nextStatus);

            if (!isClosedCorrectiveActionStatus(nextStatus) && hasActionIndex) {
              updateCorrectiveActionStatusInStore(kvId, actionIndex, nextStatus);
            }

            try {
              const response = await fetch('/api/corrective-actions/' + encodeURIComponent(actionId) + '/status', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ status: nextStatus })
              });

              const payload = await response.json().catch(function() { return {}; });
              if (!response.ok) {
                throw new Error(payload.error || 'Could not update corrective action status.');
              }

              const savedStatus = getCanonicalCorrectiveActionStatus(payload.status || nextStatus);
              syncHistoryActionStatusInCard(kvId, actionId, savedStatus);
              selectEl.dataset.previousStatus = savedStatus;

              if (isClosedCorrectiveActionStatus(savedStatus)) {
                if (hasActionIndex) {
                  removeCorrectiveActionFromStore(kvId, actionIndex);
                }
                openHistoryModal(kvId);
              } else {
                if (hasActionIndex) {
                  updateCorrectiveActionStatusInStore(kvId, actionIndex, savedStatus);
                }
                applyHistoryStatusSelectState(selectEl, savedStatus);
              }
            } catch (error) {
              if (hasActionIndex) {
                updateCorrectiveActionStatusInStore(kvId, actionIndex, previousStatus);
              }
              applyHistoryStatusSelectState(selectEl, previousStatus);
              alert(error.message || 'Could not update corrective action status.');
            } finally {
              selectEl.disabled = false;
            }
          }

          function bindHistoryStatusControls() {
            document.querySelectorAll(".history-status-select").forEach(function(selectEl) {
              applyHistoryStatusSelectState(selectEl, selectEl.value);
              selectEl.onchange = function() {
                handleHistoryStatusChange(this);
              };
            });
          }

          function closeHistoryModal() {
            const historyModal = document.getElementById("historyModal");
            if (!historyModal) return;
            historyModal.classList.remove("active"); historyModal.setAttribute("aria-hidden","true");
            if (!document.querySelector(".chart-modal-overlay.active") && !document.querySelector(".ca-modal-overlay.active")) {
              document.body.classList.remove("chart-modal-open");
            }
          }

          function getHistoryPeriodLabel(action) {
            if (!action) return "Current month";

            const monthLabel = String(action.month_label || "").trim();
            if (monthLabel) return monthLabel;

            const weekLabel = String(action.week || "").trim();
            if (!weekLabel) return "Current month";

            return weekToMonthLabelClient(weekLabel) || weekLabel;
          }

          function hasCorrectiveActionContent(action) {
            if (!action) return false;
            return Boolean(
              String(action.root_cause || "").trim() ||
              String(action.implemented_solution || "").trim() ||
              String(action.due_date || "").trim() ||
              String(action.id || action.corrective_action_id || "").trim()
            );
          }

          function shouldShowCurrentCorrectiveAction(action) {
            return hasCorrectiveActionContent(action) && !isClosedCorrectiveActionStatus(action.status);
          }

          function removeCorrectiveActionFromStore(kvId, actionIndex) {
            const actions = getCaModalActions(kvId);
            if (!Array.isArray(actions) || actionIndex < 0 || actionIndex >= actions.length) {
              return null;
            }

            const removedAction = actions.splice(actionIndex, 1)[0] || null;
            syncDomFromStore(kvId);

            if (caModalKvId === kvId) {
              renderCaModalTable(kvId);
            }

            return removedAction;
          }

          function renderHistoryActionsTable(actions, emptyMessage, options) {
            if (!actions.length) {
              return '<div class="history-empty">' + escapeHistoryHtml(emptyMessage) + '</div>';
            }

            const dueDateFrom = escapeHistoryHtml(options?.dueDateFrom || "");
            const dueDateTo = escapeHistoryHtml(options?.dueDateTo || "");

            return '' +
              '<div data-history-filter-panel>' +
                '<div class="history-filter-bar">' +
                  '<div class="history-filter-group">' +
                    '<label class="history-filter-label" for="historyDueFrom">Due Date From</label>' +
                    '<input id="historyDueFrom" class="history-filter-input" data-history-filter="due-from" type="date" value="' + dueDateFrom + '">' +
                  '</div>' +
                  '<div class="history-filter-group">' +
                    '<label class="history-filter-label" for="historyDueTo">Due Date To</label>' +
                    '<input id="historyDueTo" class="history-filter-input" data-history-filter="due-to" type="date" value="' + dueDateTo + '">' +
                  '</div>' +
                  '<button type="button" class="history-filter-clear" data-history-filter="clear">Clear Filter</button>' +
                '</div>' +
                '<div class="history-empty" data-history-filter-empty hidden>No corrective actions match the selected due date range.</div>' +
                '<div class="history-table-wrap" data-history-table-wrap>' +
                  '<table class="history-table">' +
                  '<thead>' +
                    '<tr>' +
                      '<th>#</th>' +
                      '<th>Month</th>' +
                      '<th>Root Cause</th>' +
                      '<th>Implemented Solution</th>' +
                      '<th>Due Date</th>' +
                      '<th>Responsible</th>' +
                      '<th>Status</th>' +
                      '</tr>' +
                    '</thead>' +
                    '<tbody>' +
                      actions.map(function(action, index) {
                        const filterDueDate = getHistoryDueDateFilterValue(action.due_date);
                        const dueDateDisplay = filterDueDate || String(action.due_date || "").trim();
                        return '' +
                          '<tr data-history-due-date="' + escapeHistoryHtml(filterDueDate) + '">' +
                            '<td class="history-row-number">' + (index + 1) + '</td>' +
                            '<td>' + escapeHistoryHtml(getHistoryPeriodLabel(action)) + '</td>' +
                            '<td><pre>' + escapeHistoryHtml(action.root_cause || "") + '</pre></td>' +
                            '<td><pre>' + escapeHistoryHtml(action.implemented_solution || "") + '</pre></td>' +
                            '<td>' + escapeHistoryHtml(dueDateDisplay) + '</td>' +
                            '<td>' + escapeHistoryHtml(action.responsible || "") + '</td>' +
                            '<td>' + renderHistoryStatusContent(action, options) + '</td>' +
                          '</tr>';
                      }).join("") +
                    '</tbody>' +
                  '</table>' +
                '</div>' +
              '</div>';
          }

        function openHistoryModal(kvId) {
  const historyModal = document.getElementById("historyModal");
  const historyModalTitle = document.getElementById("historyModalTitle");
  const historyModalSubtitle = document.getElementById("historyModalSubtitle");
  const historyModalContent = document.getElementById("historyModalContent");
  const card = document.querySelector('.kpi-card[data-kpi-values-id="' + kvId + '"]');

  if (!card || !historyModal || !historyModalContent) return;

  const activeModalKvId = String(historyModal.dataset.kpiValuesId || "").trim();
  const dueDateFromInput = activeModalKvId === String(kvId)
    ? historyModalContent.querySelector('[data-history-filter="due-from"]')
    : null;
  const dueDateToInput = activeModalKvId === String(kvId)
    ? historyModalContent.querySelector('[data-history-filter="due-to"]')
    : null;
  const dueDateFrom = String(dueDateFromInput?.value || "").trim();
  const dueDateTo = String(dueDateToInput?.value || "").trim();

  const titleText = getTrimmedText(card.querySelector(".kpi-title")) || "Corrective Actions";
  const subtitleText = getTrimmedText(card.querySelector(".kpi-subtitle"));
  const currentMonthLabel = card.dataset.currentMonthLabel || "Current month";
  const liveActions = getCaModalActions(kvId)
    .map(function(action, actionIndex) {
      return Object.assign({
        week: currentMonthLabel,
        month_label: currentMonthLabel,
        status: action.status || "Open",
        _editableStatus: true,
        _storeIndex: actionIndex
      }, action);
    })
    .filter(shouldShowCurrentCorrectiveAction);
  const historyActions = decodeModalPayload(card.dataset.historyActions, []);
  const historyComments = decodeModalPayload(card.dataset.historyComments, []);
  const liveActionIds = new Set(
    liveActions
      .map(function(action) {
        return String((action && (action.id || action.corrective_action_id)) || "").trim();
      })
      .filter(Boolean)
  );
  const combinedActions = liveActions.concat(
    (Array.isArray(historyActions) ? historyActions : [])
      .filter(hasCorrectiveActionContent)
      .filter(function(action) {
        return !isClosedCorrectiveActionStatus(action.status);
      })
      .filter(function(action) {
        const actionId = String((action && (action.id || action.corrective_action_id)) || "").trim();
        return !actionId || !liveActionIds.has(actionId);
      })
      .map(function(action) {
        const actionId = String((action && (action.id || action.corrective_action_id)) || "").trim();
        return Object.assign({ _editableStatus: Boolean(actionId) }, action);
      })
  );
  const sections = [];

  if (historyModalTitle) historyModalTitle.textContent = titleText;
  if (historyModalSubtitle) {
    historyModalSubtitle.textContent = subtitleText
      ? currentMonthLabel + ' • ' + subtitleText
      : currentMonthLabel;
  }

  sections.push(
    '<div class="history-section">' +
      '<h4 class="history-section-title">Corrective Actions</h4>' +
      renderHistoryActionsTable(
        combinedActions,
        'No corrective actions were found for this KPI.',
        { editableStatus: true, kvId: kvId, dueDateFrom: dueDateFrom, dueDateTo: dueDateTo }
      ) +
    '</div>'
  );

  historyModalContent.innerHTML = sections.join('');
  historyModal.dataset.kpiValuesId = String(kvId);
  bindHistoryStatusControls();
  bindHistoryDueDateFilters();
  historyModal.classList.add("active");
  historyModal.setAttribute("aria-hidden", "false");
  document.body.classList.add("chart-modal-open");
}

          /* â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
             DOM READY
          â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• */
        document.addEventListener("DOMContentLoaded", () => {
  // Value inputs
  document.querySelectorAll(".value-input").forEach(input => {
    const kvId = input.dataset.kpiValuesId;
    setValueInputServerState(input, input.value);
    checkLowLimit(input);
    buildKpiChart(kvId);

    input.addEventListener("input", function() {
      syncValueInputDirtyState(this);
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

            // Card action bar â€” View Previous Actions
            document.querySelectorAll(".view-prev-ca-btn").forEach(btn => {
              const kvId = btn.dataset.kpiValuesId; if (!kvId) return;
              btn.addEventListener("click", () => openHistoryModal(kvId));
            });

            // Card action bar â€” Open CA Table Modal
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

            const closeActionConfirmModal = document.getElementById("closeActionConfirmModal");
            const closeActionConfirmCancel = document.getElementById("closeActionConfirmCancel");
            const closeActionConfirmConfirm = document.getElementById("closeActionConfirmConfirm");
            if (closeActionConfirmCancel) {
              closeActionConfirmCancel.addEventListener("click", () => settleCloseActionConfirm(false));
            }
            if (closeActionConfirmConfirm) {
              closeActionConfirmConfirm.addEventListener("click", () => settleCloseActionConfirm(true));
            }
            if (closeActionConfirmModal) {
              closeActionConfirmModal.addEventListener("click", e => {
                if (e.target === closeActionConfirmModal) {
                  settleCloseActionConfirm(false);
                }
              });
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
                if (document.getElementById("closeActionConfirmModal").classList.contains("active")) { settleCloseActionConfirm(false); return; }
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
          window.openHistoryModal = openHistoryModal;
          window.openCaTableModal = openCaTableModal;
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
    const responsibleId = normalizeOptionalIntegerInput(req.query?.responsible_id);
    if (!responsibleId) {
      return res.status(400).send("Missing responsible_id");
    }
    const responsible_id = responsibleId;

    const responsibleContext = await loadFormResponsibleContext(responsible_id);
    const responsiblePeopleId = normalizeOptionalIntegerInput(
      responsibleContext?.people_id ?? responsible_id
    );

    if (!responsiblePeopleId) {
      throw new Error("Responsible not found");
    }

    const responsible = {
      name: responsibleContext?.name || `Person #${responsiblePeopleId}`,
      plant_name: responsibleContext?.plant_name || "Allocated scope",
      department_name: responsibleContext?.department_name || ""
    };

    const kpiRes = await pool.query(
      `
      WITH ranked_results AS (
        SELECT
          kr.kpi_result_id,
          kr.kpi_target_allocation_id AS kpi_values_id,
          kr.kpi_target_allocation_id,
          kr.kpi_id,
          kr.result_date,
          kr.week,
          kr.period_label,
          kr.raw_value AS value,
          kr.lower_limit AS result_low_limit,
          kr.upper_limit AS result_high_limit,
          kr.target_value AS result_target_value,
          kr.unit AS result_unit,
          kr.recorded_at AS updated_at,
          DATE_TRUNC('month', COALESCE(kr.result_date::timestamp, kr.recorded_at)) AS month_bucket,
          ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('month', COALESCE(kr.result_date::timestamp, kr.recorded_at)), kr.kpi_target_allocation_id
            ORDER BY COALESCE(kr.recorded_at, kr.result_date::timestamp) DESC, kr.kpi_result_id DESC
          ) AS rn
        FROM public.kpi_result kr
        WHERE kr.recorded_by_people_id = $1
      )
      SELECT
        rr.kpi_result_id AS hist_id,
        rr.kpi_values_id,
        rr.kpi_target_allocation_id,
        rr.week,
        rr.period_label,
        rr.result_date,
        rr.kpi_id,
        rr.value,
        rr.updated_at,
        COALESCE(k.kpi_name, k.kpi_sub_title, 'KPI') AS subject,
        COALESCE(subject_link.subject_name, k.kpi_sub_title, '') AS indicator_sub_title,
        COALESCE(rr.result_unit, kta.target_unit, k.unit) AS unit,
        COALESCE(rr.result_target_value, kta.target_value, k.target_value) AS target,
        k.min_value AS min,
        k.max_value AS max,
        k.tolerance_type,
        k.up_tolerance,
        k.low_tolerance,
        k.frequency,
        k.kpi_explanation AS definition,
        k.calculation_on,
        k.target_auto_adjustment,
        COALESCE(rr.result_high_limit, k.high_limit) AS high_limit,
        COALESCE(rr.result_low_limit, k.low_limit) AS low_limit,
        k.target_direction
      FROM ranked_results rr
      JOIN public.kpi k
        ON k.kpi_id = rr.kpi_id
      LEFT JOIN public.kpi_target_allocation kta
        ON kta.kpi_target_allocation_id = rr.kpi_target_allocation_id
      LEFT JOIN LATERAL (
        SELECT s.subject_name
        FROM public.subject_kpi sk
        JOIN public.subject s
          ON s.subject_id = sk.subject_id
        WHERE sk.kpi_id = rr.kpi_id
        ORDER BY COALESCE(sk.is_primary, false) DESC, sk.subject_kpi_id ASC
        LIMIT 1
      ) subject_link ON TRUE
      WHERE rr.rn = 1
      ORDER BY
        rr.month_bucket DESC,
        COALESCE(k.kpi_name, k.kpi_sub_title, 'KPI') ASC,
        rr.kpi_result_id DESC
      `,
      [responsiblePeopleId]
    );

    const monthMap = new Map();

    kpiRes.rows.forEach((kpi) => {
      const sourceDate = kpi.result_date || kpi.updated_at;
      if (!sourceDate) return;

      const date = new Date(sourceDate);
      if (isNaN(date.getTime())) return;
      const monthKey = `${date.getFullYear()}-${date.getMonth()}`;

      if (!monthMap.has(monthKey)) monthMap.set(monthKey, []);
      monthMap.get(monthKey).push(kpi);
    });

    const sortedMonthEntries = Array.from(monthMap.entries()).sort((a, b) => {
      const [yearA, monthA] = a[0].split("-").map(Number);
      const [yearB, monthB] = b[0].split("-").map(Number);
      return new Date(yearB, monthB) - new Date(yearA, monthA);
    });

    const formatDashboardMetricValue = (value, unit = "") => {
      const numericValue = normalizeMetricNumberByUnit(value, unit);
      if (numericValue === null) return "Not filled";
      const formatted = numericValue.toLocaleString("en-US", {
        useGrouping: false,
        minimumFractionDigits: 0,
        maximumFractionDigits: isPercentageUnit(unit) ? 1 : 2
      });
      return unit ? `${formatted} ${unit}` : formatted;
    };

    const formatDashboardTimestamp = (value) => {
      if (!value) return "No updates recorded";
      const parsed = new Date(value);
      if (isNaN(parsed.getTime())) return "No updates recorded";
      return parsed.toLocaleString("en-GB", {
        day: "2-digit",
        month: "short",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit"
      });
    };

    const monthSummaries = sortedMonthEntries.map(([monthKey, items], index) => {
      const [year, month] = monthKey.split("-").map(Number);
      const monthDate = new Date(year, month);
      let alertCount = 0;
      let onTrackCount = 0;
      let missingCount = 0;
      let latestUpdatedAt = null;

      items.forEach((kpi) => {
        const currentValue = normalizeMetricNumberByUnit(kpi.value, kpi.unit);
        const lowLimit = normalizeMetricNumberByUnit(kpi.low_limit, kpi.unit);
        const highLimit = normalizeMetricNumberByUnit(kpi.high_limit, kpi.unit);
        const statusInfo = getKpiStatus(currentValue, lowLimit, highLimit, inferKpiDirection(kpi));

        if (currentValue === null) missingCount += 1;
        else if (statusInfo.isGood === false) alertCount += 1;
        else onTrackCount += 1;

        if (kpi.updated_at) {
          const updatedAt = new Date(kpi.updated_at);
          if (!isNaN(updatedAt.getTime()) && (!latestUpdatedAt || updatedAt > latestUpdatedAt)) {
            latestUpdatedAt = updatedAt;
          }
        }
      });

      return {
        monthKey,
        monthLabel: monthDate.toLocaleString("en-US", { month: "long", year: "numeric" }),
        monthShort: monthDate.toLocaleString("en-US", { month: "short" }).toUpperCase(),
        isLatest: index === 0,
        kpiCount: items.length,
        alertCount,
        onTrackCount,
        missingCount,
        lastUpdated: formatDashboardTimestamp(latestUpdatedAt)
      };
    });

    const monthSummaryByKey = new Map(monthSummaries.map((summary) => [summary.monthKey, summary]));
    const selectedMonthSummary = monthSummaries[0] || null;
    const selectedMonthKey = selectedMonthSummary?.monthKey || "";
    const monthOptionsHtml = monthSummaries
      .map((summary) =>
        `<option value="${escapeHtml(summary.monthKey)}"${summary.monthKey === selectedMonthKey ? " selected" : ""}>${escapeHtml(summary.monthLabel)}${summary.isLatest ? " - Latest" : ""}</option>`
      )
      .join("");
    const monthSummaryMapJson = JSON.stringify(
      monthSummaries.reduce((acc, summary) => {
        acc[summary.monthKey] = summary;
        return acc;
      }, {})
    ).replace(/</g, "\\u003c");

    let html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>KPI Dashboard</title>
  <style>
    :root{
      --bg:#eef4f9;
      --panel:#ffffff;
      --panel-soft:#f8fbff;
      --text:#0f172a;
      --muted:#64748b;
      --border:rgba(148,163,184,0.22);
      --brand:#0f6cbd;
      --brand-strong:#005a9c;
      --good:#16a34a;
      --warn:#f59e0b;
      --bad:#dc2626;
      --shadow:0 24px 60px rgba(15,23,42,0.14);
    }
    *{box-sizing:border-box;}
    body{
      font-family:'Segoe UI',sans-serif;
      background:
        radial-gradient(circle at top left, rgba(15,108,189,0.18), transparent 28%),
        linear-gradient(180deg, #eef4f9 0%, #f8fbfd 100%);
      padding:30px 20px 40px;
      margin:0;
      color:var(--text);
    }
    .container{max-width:1120px;margin:0 auto;}
    .header{
      position:relative;
      overflow:hidden;
      background:linear-gradient(135deg, var(--brand) 0%, #2894ff 100%);
      color:white;
      padding:34px 36px 28px;
      border-radius:28px 28px 0 0;
    }
    .header::after{
      content:"";
      position:absolute;
      inset:auto -120px -120px auto;
      width:320px;
      height:320px;
      background:radial-gradient(circle, rgba(255,255,255,0.26) 0%, rgba(255,255,255,0) 70%);
      pointer-events:none;
    }
    .header-eyebrow{
      display:inline-flex;
      align-items:center;
      padding:8px 12px;
      border-radius:999px;
      font-size:12px;
      letter-spacing:0.08em;
      text-transform:uppercase;
      background:rgba(255,255,255,0.14);
      border:1px solid rgba(255,255,255,0.18);
      margin-bottom:16px;
    }
    .header h1{
      margin:0;
      font-size:38px;
      line-height:1.1;
      font-weight:700;
      letter-spacing:-0.03em;
    }
    .header p{
      margin:12px 0 0;
      max-width:680px;
      font-size:15px;
      line-height:1.6;
      color:rgba(255,255,255,0.88);
    }
    .content{
      background:rgba(255,255,255,0.96);
      backdrop-filter:blur(10px);
      padding:28px;
      border-radius:0 0 28px 28px;
      box-shadow:var(--shadow);
    }
    .identity-grid{
      display:grid;
      grid-template-columns:repeat(3,minmax(0,1fr));
      gap:14px;
      margin-bottom:20px;
    }
    .identity-card{
      padding:18px 18px 16px;
      border-radius:20px;
      background:linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
      border:1px solid var(--border);
      box-shadow:0 10px 26px rgba(148,163,184,0.10);
    }
    .identity-label{
      font-size:12px;
      font-weight:700;
      letter-spacing:0.08em;
      text-transform:uppercase;
      color:var(--muted);
      margin-bottom:8px;
    }
    .identity-value{
      font-size:20px;
      font-weight:700;
      color:var(--text);
      line-height:1.3;
    }
    .toolbar{
      display:flex;
      justify-content:space-between;
      align-items:flex-end;
      gap:18px;
      padding:20px 22px;
      border-radius:22px;
      background:linear-gradient(180deg, #ffffff 0%, #f7fbff 100%);
      border:1px solid var(--border);
      margin-bottom:20px;
    }
    .toolbar-copy{max-width:560px;}
    .toolbar-label{
      font-size:12px;
      font-weight:700;
      letter-spacing:0.08em;
      text-transform:uppercase;
      color:var(--brand);
      margin-bottom:6px;
    }
    .toolbar-title{
      font-size:24px;
      font-weight:700;
      letter-spacing:-0.02em;
      color:var(--text);
      margin-bottom:6px;
    }
    .toolbar-text{
      margin:0;
      color:var(--muted);
      line-height:1.6;
      font-size:14px;
    }
    .month-filter{
      min-width:260px;
      display:flex;
      flex-direction:column;
      gap:8px;
    }
    .month-filter span{
      font-size:12px;
      font-weight:700;
      letter-spacing:0.08em;
      text-transform:uppercase;
      color:var(--muted);
    }
    .month-filter select{
      width:100%;
      padding:14px 16px;
      border-radius:16px;
      border:1px solid rgba(15,108,189,0.18);
      background:#fff;
      color:var(--text);
      font-size:15px;
      font-weight:600;
      outline:none;
      box-shadow:0 8px 20px rgba(15,108,189,0.08);
    }
    .active-month-card{
      display:flex;
      justify-content:space-between;
      align-items:flex-start;
      gap:20px;
      padding:24px 26px;
      border-radius:24px;
      background:
        linear-gradient(135deg, rgba(15,108,189,0.08) 0%, rgba(40,148,255,0.05) 100%),
        #ffffff;
      border:1px solid rgba(15,108,189,0.16);
      margin-bottom:22px;
      box-shadow:0 16px 34px rgba(15,108,189,0.08);
    }
    .active-month-badge{
      display:inline-flex;
      align-items:center;
      padding:8px 12px;
      border-radius:999px;
      background:rgba(15,108,189,0.10);
      color:var(--brand-strong);
      font-size:12px;
      font-weight:700;
      letter-spacing:0.06em;
      text-transform:uppercase;
      margin-bottom:12px;
    }
    .active-month-title{
      margin:0;
      font-size:34px;
      line-height:1.08;
      letter-spacing:-0.03em;
      color:var(--text);
    }
    .active-month-description{
      margin:10px 0 0;
      max-width:560px;
      color:var(--muted);
      font-size:15px;
      line-height:1.6;
    }
    .active-month-stats{
      display:grid;
      grid-template-columns:repeat(2,minmax(150px,1fr));
      gap:12px;
      min-width:340px;
    }
    .active-stat{
      padding:14px 16px;
      border-radius:18px;
      background:rgba(255,255,255,0.9);
      border:1px solid rgba(148,163,184,0.18);
    }
    .active-stat-label{
      font-size:12px;
      font-weight:700;
      letter-spacing:0.07em;
      text-transform:uppercase;
      color:var(--muted);
      margin-bottom:8px;
    }
    .active-stat strong{
      display:block;
      font-size:28px;
      line-height:1;
      color:var(--text);
    }
    .active-stat small{
      display:block;
      margin-top:8px;
      color:var(--muted);
      font-size:12px;
      line-height:1.5;
    }
    .month-section{
      margin-bottom:24px;
      padding:22px;
      border-radius:24px;
      background:var(--panel-soft);
      border:1px solid var(--border);
    }
    .month-section[hidden]{display:none !important;}
    .month-section-header{
      display:flex;
      justify-content:space-between;
      align-items:flex-start;
      gap:18px;
      margin-bottom:18px;
    }
    .month-heading{
      display:flex;
      align-items:center;
      gap:14px;
    }
    .month-calendar{
      width:58px;
      height:58px;
      border-radius:18px;
      display:flex;
      align-items:center;
      justify-content:center;
      background:linear-gradient(135deg, var(--brand) 0%, #3fa7ff 100%);
      color:#fff;
      font-size:14px;
      font-weight:800;
      letter-spacing:0.08em;
      box-shadow:0 14px 24px rgba(15,108,189,0.18);
    }
    .month-title{
      margin:0;
      font-size:28px;
      line-height:1.1;
      letter-spacing:-0.03em;
      color:var(--text);
    }
    .month-subtitle{
      margin:6px 0 0;
      color:var(--muted);
      font-size:14px;
    }
    .month-badges{
      display:flex;
      flex-wrap:wrap;
      gap:10px;
    }
    .month-badge{
      display:inline-flex;
      align-items:center;
      gap:8px;
      padding:10px 14px;
      border-radius:999px;
      background:#fff;
      border:1px solid var(--border);
      color:var(--muted);
      font-size:13px;
      font-weight:600;
    }
    .month-badge strong{color:var(--text);}
    .kpi-list{
      display:grid;
      gap:14px;
    }
    .kpi-card{
      background:#fff;
      border:1px solid rgba(148,163,184,0.18);
      border-radius:20px;
      padding:18px 20px;
      box-shadow:0 12px 26px rgba(148,163,184,0.08);
    }
    .kpi-title{
      font-weight:700;
      color:var(--text);
      margin:0 0 6px;
      font-size:18px;
    }
    .kpi-top{
      display:flex;
      justify-content:space-between;
      align-items:flex-start;
      gap:18px;
    }
    .kpi-subtitle{
      color:var(--muted);
      font-size:14px;
      line-height:1.55;
      margin:0;
      font-style:italic;
    }
    .status-chip{
      display:inline-flex;
      align-items:center;
      gap:8px;
      padding:9px 12px;
      border-radius:999px;
      font-size:12px;
      font-weight:700;
      letter-spacing:0.06em;
      text-transform:uppercase;
      white-space:nowrap;
      border:1px solid transparent;
    }
    .status-chip.good{
      background:rgba(22,163,74,0.10);
      color:var(--good);
      border-color:rgba(22,163,74,0.16);
    }
    .status-chip.bad{
      background:rgba(220,38,38,0.10);
      color:var(--bad);
      border-color:rgba(220,38,38,0.16);
    }
    .status-chip.neutral{
      background:rgba(100,116,139,0.10);
      color:var(--muted);
      border-color:rgba(100,116,139,0.16);
    }
    .status-dot{
      width:8px;
      height:8px;
      border-radius:50%;
      background:currentColor;
    }
    .metric-grid{
      display:grid;
      grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
      gap:12px;
      margin-top:16px;
    }
    .metric-card{
      padding:14px 15px;
      border-radius:16px;
      background:linear-gradient(180deg,#ffffff 0%,#f9fbfd 100%);
      border:1px solid rgba(148,163,184,0.18);
    }
    .metric-label{
      font-size:11px;
      font-weight:700;
      letter-spacing:0.08em;
      text-transform:uppercase;
      color:var(--muted);
      margin-bottom:8px;
    }
    .metric-value{
      font-size:24px;
      font-weight:700;
      line-height:1.1;
      color:var(--text);
      letter-spacing:-0.02em;
    }
    .metric-value.good{color:var(--good);}
    .metric-value.bad{color:var(--bad);}
    .metric-value.warn{color:var(--warn);}
    .kpi-footer{
      display:flex;
      justify-content:space-between;
      align-items:center;
      gap:10px;
      margin-top:16px;
      padding-top:14px;
      border-top:1px solid rgba(148,163,184,0.16);
      color:var(--muted);
      font-size:12px;
    }
    .empty-state{
      padding:34px 24px;
      border-radius:24px;
      text-align:center;
      background:linear-gradient(180deg,#ffffff 0%,#f8fbff 100%);
      border:1px solid var(--border);
      color:var(--muted);
      font-size:15px;
    }
    @media (max-width: 900px){
      .identity-grid,
      .active-month-stats{
        grid-template-columns:1fr;
      }
      .toolbar,
      .active-month-card,
      .month-section-header,
      .kpi-top,
      .kpi-footer{
        flex-direction:column;
        align-items:flex-start;
      }
      .month-filter{
        width:100%;
        min-width:0;
      }
    }
    @media (max-width: 640px){
      body{padding:18px 12px 24px;}
      .header,
      .content{padding:22px 18px;}
      .header h1{font-size:30px;}
      .active-month-title,
      .month-title{font-size:24px;}
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="header-eyebrow">KPI Performance Center</div>
      <h1>${escapeHtml(responsible.name)} Dashboard</h1>
      <p>Review KPI snapshots by month, focus on the latest performance picture, and switch between archived views without leaving the page.</p>
    </div>

    <div class="content">
      <div class="identity-grid">
        <div class="identity-card">
          <div class="identity-label">Responsible</div>
          <div class="identity-value">${escapeHtml(responsible.name || "-")}</div>
        </div>
        <div class="identity-card">
          <div class="identity-label">Group</div>
          <div class="identity-value">${escapeHtml(responsible.plant_name || "")}</div>
        </div>
        <div class="identity-card">
          <div class="identity-label">Department</div>
          <div class="identity-value">${escapeHtml(responsible.department_name || "-")}</div>
        </div>
      </div>`;

    if (sortedMonthEntries.length === 0) {
      html += `<div class="empty-state">No KPI data available yet.</div>`;
    } else {
      html += `
      <div class="toolbar">
        <div class="toolbar-copy">
          <div class="toolbar-label">Monthly View</div>
          <div class="toolbar-title">Filter KPI snapshots by month</div>
          <p class="toolbar-text">Use the selector to focus on one reporting month at a time. The dashboard summary below updates instantly to match your selection.</p>
        </div>
        <label class="month-filter" for="monthFilter">
          <span>Select month</span>
          <select id="monthFilter">${monthOptionsHtml}</select>
        </label>
      </div>

      <section class="active-month-card" id="activeMonthCard">
        <div>
          <div class="active-month-badge">Current Selection</div>
          <h2 class="active-month-title" id="activeMonthLabel">${escapeHtml(selectedMonthSummary?.monthLabel || "Latest month")}</h2>
          <p class="active-month-description" id="activeMonthDescription">Showing the KPI snapshot recorded for ${escapeHtml(selectedMonthSummary?.monthLabel || "the latest available month")}.</p>
        </div>
        <div class="active-month-stats">
          <div class="active-stat">
            <div class="active-stat-label">KPI Cards</div>
            <strong id="activeMonthKpiCount">${selectedMonthSummary?.kpiCount ?? 0}</strong>
            <small>Total KPI snapshots in this month</small>
          </div>
          <div class="active-stat">
            <div class="active-stat-label">Needs Attention</div>
            <strong id="activeMonthAlertCount">${selectedMonthSummary?.alertCount ?? 0}</strong>
            <small>KPIs currently outside their expected direction</small>
          </div>
          <div class="active-stat">
            <div class="active-stat-label">On Track</div>
            <strong id="activeMonthHealthyCount">${selectedMonthSummary?.onTrackCount ?? 0}</strong>
            <small>KPIs aligned with their expected direction</small>
          </div>
          <div class="active-stat">
            <div class="active-stat-label">Last Update</div>
            <strong id="activeMonthLastUpdated">${escapeHtml(selectedMonthSummary?.lastUpdated || "No updates recorded")}</strong>
            <small>Most recent KPI update stored for this month</small>
          </div>
        </div>
      </section>`;

      for (const [monthKey, items] of sortedMonthEntries) {
        const [year, month] = monthKey.split("-").map(Number);
        const date = new Date(year, month);
        const monthLabel = date.toLocaleString("en-US", { month: "long", year: "numeric" });
        const summary = monthSummaryByKey.get(monthKey);

        html += `
        <section class="month-section" data-month-key="${escapeHtml(monthKey)}"${monthKey === selectedMonthKey ? "" : " hidden"}>
          <div class="month-section-header">
            <div class="month-heading">
              <div class="month-calendar">${escapeHtml(summary?.monthShort || date.toLocaleString("en-US", { month: "short" }).toUpperCase())}</div>
              <div>
                <h2 class="month-title">${escapeHtml(monthLabel)}</h2>
                <p class="month-subtitle">${items.length} KPI snapshot${items.length !== 1 ? "s" : ""} recorded for this view</p>
              </div>
            </div>
            <div class="month-badges">
              <div class="month-badge"><strong>${summary?.alertCount ?? 0}</strong> need attention</div>
              <div class="month-badge"><strong>${summary?.onTrackCount ?? 0}</strong> on track</div>
              <div class="month-badge"><strong>${escapeHtml(summary?.lastUpdated || "No updates")}</strong></div>
            </div>
          </div>
          <div class="kpi-list">`;

        items.forEach((kpi) => {
          const currentValue = normalizeMetricNumberByUnit(kpi.value, kpi.unit);
          const targetValue = normalizeMetricNumberByUnit(kpi.target, kpi.unit);
          const lowLimit = normalizeMetricNumberByUnit(kpi.low_limit, kpi.unit);
          const highLimit = normalizeMetricNumberByUnit(kpi.high_limit, kpi.unit);
          const statusInfo = getKpiStatus(currentValue, lowLimit, highLimit, inferKpiDirection(kpi));
          const hasValue = currentValue !== null;
          const statusLabel = !hasValue
            ? "Pending update"
            : statusInfo.isGood === false
              ? "Needs attention"
              : "On track";
          const statusClass = !hasValue
            ? "neutral"
            : statusInfo.isGood === false
              ? "bad"
              : "good";
          const actualValueClass = !hasValue
            ? ""
            : statusInfo.isGood === false
              ? "bad"
              : "good";

          html += `
            <div class="kpi-card">
              <div class="kpi-top">
                <div>
                  <h3 class="kpi-title">${escapeHtml(kpi.subject || "-")}</h3>
                  <p class="kpi-subtitle">${escapeHtml(kpi.indicator_sub_title || "No KPI subtitle provided.")}</p>
                </div>
                <div class="status-chip ${statusClass}">
                  <span class="status-dot"></span>
                  <span>${statusLabel}</span>
                </div>
              </div>
              <div class="metric-grid">
                <div class="metric-card">
                  <div class="metric-label">Actual</div>
                  <div class="metric-value ${actualValueClass}">${escapeHtml(formatDashboardMetricValue(currentValue, kpi.unit))}</div>
                </div>
                ${targetValue !== null
              ? `<div class="metric-card">
                      <div class="metric-label">Target</div>
                      <div class="metric-value good">${escapeHtml(formatDashboardMetricValue(targetValue, kpi.unit))}</div>
                    </div>`
              : ""}
                ${highLimit !== null
              ? `<div class="metric-card">
                      <div class="metric-label">High Limit</div>
                      <div class="metric-value warn">${escapeHtml(formatDashboardMetricValue(highLimit, kpi.unit))}</div>
                    </div>`
              : ""}
                ${lowLimit !== null
              ? `<div class="metric-card">
                      <div class="metric-label">Low Limit</div>
                      <div class="metric-value bad">${escapeHtml(formatDashboardMetricValue(lowLimit, kpi.unit))}</div>
                    </div>`
              : ""}
              </div>
              <div class="kpi-footer">
                <div>Last updated: ${escapeHtml(formatDashboardTimestamp(kpi.updated_at))}</div>
                <div>${escapeHtml(responsible.department_name || "")}</div>
              </div>
            </div>`;
        });

        html += `</div></section>`;
      }
    }

    html += `
    <script>
      (function () {
        const monthFilter = document.getElementById("monthFilter");
        const monthSections = Array.from(document.querySelectorAll(".month-section"));
        const monthSummaryMap = ${monthSummaryMapJson};
        const activeMonthLabel = document.getElementById("activeMonthLabel");
        const activeMonthDescription = document.getElementById("activeMonthDescription");
        const activeMonthKpiCount = document.getElementById("activeMonthKpiCount");
        const activeMonthAlertCount = document.getElementById("activeMonthAlertCount");
        const activeMonthHealthyCount = document.getElementById("activeMonthHealthyCount");
        const activeMonthLastUpdated = document.getElementById("activeMonthLastUpdated");
        const defaultMonthKey = ${JSON.stringify(selectedMonthKey)};

        function applyMonthFilter(monthKey) {
          const resolvedKey = monthSummaryMap[monthKey] ? monthKey : defaultMonthKey;
          monthSections.forEach((section) => {
            section.hidden = section.dataset.monthKey !== resolvedKey;
          });

          const summary = monthSummaryMap[resolvedKey];
          if (!summary) return;

          if (monthFilter && monthFilter.value !== resolvedKey) {
            monthFilter.value = resolvedKey;
          }

          if (activeMonthLabel) activeMonthLabel.textContent = summary.monthLabel;
          if (activeMonthDescription) {
            activeMonthDescription.textContent = "Showing the KPI snapshot recorded for " + summary.monthLabel + ".";
          }
          if (activeMonthKpiCount) activeMonthKpiCount.textContent = summary.kpiCount;
          if (activeMonthAlertCount) activeMonthAlertCount.textContent = summary.alertCount;
          if (activeMonthHealthyCount) activeMonthHealthyCount.textContent = summary.onTrackCount;
          if (activeMonthLastUpdated) activeMonthLastUpdated.textContent = summary.lastUpdated;
        }

        if (monthFilter) {
          monthFilter.addEventListener("change", function () {
            applyMonthFilter(this.value);
          });
          applyMonthFilter(monthFilter.value || defaultMonthKey);
        }
      })();
    </script>
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
        <td>${r.old_value ?? 'â€”'}</td><td>${r.new_value ?? 'â€”'}</td>
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
    console.log(`Email sent to ${responsible.email}`);
  } catch (err) {
    console.error(`âŒ Failed to send email to responsible ID ${responsibleId}:`, err.message);
  }
};

const formatNumber = (num) => {
  const n = parseFloat(num);
  if (Number.isInteger(n)) return n.toString();
  if (Math.abs(n - Math.round(n)) < 0.0001) return Math.round(n).toString();
  return n.toFixed(1);
};

// ============================================================
// generateVerticalBarChart â€” DOTS + HIGH/LOW LIMIT LINES
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
  const thresholdLineValues = getReadableThresholdLineValues(cleanLow, cleanTarget, cleanHigh);
  const displayHigh = thresholdLineValues.displayHigh;
  const displayLow = thresholdLineValues.displayLow;
  const displayTarget = thresholdLineValues.displayTarget;

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
  const axisRange = getAutoChartAxisRange(values, displayLow, displayTarget, displayHigh);

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
  const directionMeta = getKpiGoodDirectionMeta(resolvedDirection);
  const pointColors = values.map(v => getDotColor(v, cleanLow, cleanHigh, resolvedDirection));

  const allVals = [...validData];
  if (displayHigh !== null) allVals.push(displayHigh);
  if (displayLow !== null) allVals.push(displayLow);
  if (displayTarget !== null) allVals.push(displayTarget);

  const thresholdValues = [cleanLow, cleanTarget, cleanHigh]
    .filter((value) => value !== null)
    .sort((a, b) => a - b);
  const tightThresholdCluster = thresholdLineValues.hasTightCluster;
  const chartHeightPx = tightThresholdCluster ? 320 : 260;

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
      if (cleanHigh !== null && Math.abs(val - cleanHigh) < interval / 2) ind += ' H';
      if (cleanLow !== null && Math.abs(val - cleanLow) < interval / 2) ind += ' L';
      h += `<tr><td height="${segmentHeight}" valign="top" align="right"
              style="font-size:10px;color:#666;padding-right:8px;white-space:nowrap;">
              ${fmt(val)}${ind}</td></tr>`;
    }
    return h;
  };

  const currentValue = data[data.length - 1] || 0;
  const directionGuideHtml = `
    <table border="0" cellpadding="0" cellspacing="0" width="100%"
           style="margin:0 0 18px;background:${directionMeta.background};
                  border:1px solid ${directionMeta.border};border-radius:12px;">
      <tr>
        <td style="padding:14px 16px;">
          <div style="font-size:11px;font-weight:700;color:${directionMeta.accent};
                      text-transform:uppercase;letter-spacing:0.08em;margin-bottom:6px;">
            Good Direction
          </div>
          <div style="font-size:15px;font-weight:700;color:#1f2937;margin-bottom:4px;">
            <span style="color:${directionMeta.accent};margin-right:6px;">${directionMeta.icon}</span>
            ${directionMeta.label} = ${directionMeta.summary}
          </div>
          <div style="font-size:12px;color:#475569;line-height:1.5;">
            Examples: ${directionMeta.examples}. This direction is used for colors, alerts, and target management.
          </div>
        </td>
      </tr>
    </table>
  `;

  // STATS BOX - 3 columns (CURRENT, AVERAGE, TREND)
  // Replace the statsBox const with this:
  const currentStatus = getKpiStatus(currentValue, cleanLow, cleanHigh, resolvedDirection);
  const trendIcon = currentStatus.isGood === false
    ? {
      icon: resolvedDirection === 'down' ? '&uarr;' : '&darr;',
      color: '#dc2626'
    }
    : {
      icon: resolvedDirection === 'down' ? '&darr;' : '&uarr;',
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

            <!-- TREND ICON â€” same row, between CURRENT and AVERAGE -->
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
      data: new Array(values.length).fill(displayHigh),
      borderColor: '#ff9800',
      borderWidth: 3,
      borderDash: [12, 5],
      pointRadius: 0,
      fill: false
    });
  }

  if (cleanLow !== null) {
    datasets.push({
      label: `Low Limit (${fmt(cleanLow)})`,
      data: new Array(values.length).fill(displayLow),
      borderColor: '#dc3545',
      borderWidth: 3,
      borderDash: [2, 6],
      pointRadius: 0,
      fill: false
    });
  }

  if (cleanTarget !== null) {
    datasets.push({
      label: `Target (${fmt(cleanTarget)})`,
      data: new Array(values.length).fill(displayTarget),
      borderColor: '#16a34a',
      borderWidth: 3.5,
      borderDash: [],
      pointRadius: 0,
      fill: false
    });
  }

  const chartConfig = {
    type: 'line',
    data: { labels, datasets },
    options: {
      legend: { display: false },
      layout: {
        padding: {
          top: tightThresholdCluster ? 12 : 6,
          right: 12,
          bottom: 4,
          left: 4
        }
      },
      scales: {
        xAxes: [{
          ticks: { fontSize: 10 },
          gridLines: { color: 'rgba(0,0,0,0.05)' }
        }],
        yAxes: [{
          ticks: {
            fontSize: 10,
            beginAtZero: false,
            min: axisRange.min,
            max: axisRange.max
          },
          gridLines: { color: 'rgba(0,0,0,0.05)' }
        }]
      }
    }
  };

  const chartUrl =
    `https://quickchart.io/chart?c=${encodeURIComponent(JSON.stringify(chartConfig))}` +
    `&w=500&h=${chartHeightPx}&bkg=white`;

  const commentsHtml = comments.length > 0 ? `
    <div style="margin-bottom:20px;">
      <h4 style="margin:0 0 15px;color:#333;font-size:16px;">Comments</h4>
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
      <h4 style="margin:0 0 15px;color:#333;font-size:16px;">Corrective Actions</h4>
      ${correctiveActions.map(ca => `
        <div style="margin-bottom:15px;padding:15px;background:#fff3f3;border-radius:8px;border-left:4px solid #dc3545;">
       <div style="font-size:12px;font-weight:600;color:#495057;margin-bottom:8px;">
         ${ca.week ? weekToMonthLabel(ca.week) : 'N/A'}
         ${ca.status ? `<span style="margin-left:10px;font-size:11px;color:#dc3545;">${ca.status}</span>` : ''}
          </div>
          ${ca.root_cause ? `
            <div style="margin-bottom:8px;">
              <div style="font-size:11px;font-weight:700;color:#dc3545;">Root Cause</div>
              <div style="font-size:12px;color:#374151;">${ca.root_cause}</div>
            </div>
          ` : ''}
          ${ca.implemented_solution ? `
            <div style="margin-bottom:8px;">
              <div style="font-size:11px;font-weight:700;color:#d97706;">Implemented Solution</div>
              <div style="font-size:12px;color:#374151;">${ca.implemented_solution}</div>
            </div>
          ` : ''}
        </div>
      `).join('')}
    </div>
  ` : '';

  // EMAIL-SAFE LIMITS (High + Low side-by-side using table)
  const limitsRowHtml = (() => {
    const highBox = cleanHigh !== null ? `
    <table border="0" cellpadding="0" cellspacing="0" width="100%"
           style="background:white;border-radius:12px;border:1px solid #e0e0e0;text-align:center;">
      <tr><td style="padding:15px;">
        <div style="font-size:11px;color:#666;text-transform:uppercase;margin-bottom:5px;">HIGH LIMIT</div>
        <div style="font-size:28px;font-weight:700;color:#ff9800;">${fmt(cleanHigh)}</div>
        <div style="font-size:11px;color:#999;">${unit || ''}</div>
      </td></tr>
    </table>
  ` : '';

    const targetBox = cleanTarget !== null ? `
    <table border="0" cellpadding="0" cellspacing="0" width="100%"
           style="background:white;border-radius:12px;border:1px solid #e0e0e0;text-align:center;">
      <tr><td style="padding:15px;">
        <div style="font-size:11px;color:#666;text-transform:uppercase;margin-bottom:5px;">TARGET</div>
        <div style="font-size:28px;font-weight:700;color:#16a34a;">${fmt(cleanTarget)}</div>
        <div style="font-size:11px;color:#999;">${unit || ''}</div>
      </td></tr>
    </table>
  ` : '';

    const lowBox = cleanLow !== null ? `
    <table border="0" cellpadding="0" cellspacing="0" width="100%"
           style="background:white;border-radius:12px;border:1px solid #e0e0e0;text-align:center;">
      <tr><td style="padding:15px;">
        <div style="font-size:11px;color:#666;text-transform:uppercase;margin-bottom:5px;">LOW LIMIT</div>
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

  const thresholdFocusHtml = tightThresholdCluster ? `
    <div style="margin:0 0 16px;padding:12px 14px;background:#f8fbff;border:1px solid #dbeafe;
                border-radius:12px;">
      <div style="font-size:11px;font-weight:700;color:#0f6cbd;text-transform:uppercase;
                  letter-spacing:0.08em;margin-bottom:6px;">Threshold Focus</div>
      <div style="font-size:13px;color:#475569;line-height:1.5;">
        High limit, target, and low limit are tightly grouped between
        <strong>${fmt(thresholdValues[0])}</strong> and
        <strong>${fmt(thresholdValues[thresholdValues.length - 1])}</strong>${unit ? ` ${unit}` : ''}.
        The chart lines are slightly spaced for readability, and the exact values are shown below.
      </div>
    </div>
  ` : '';

  const preChartLimitsHtml = tightThresholdCluster ? limitsRowHtml : '';
  const postChartLimitsHtml = tightThresholdCluster ? '' : limitsRowHtml;

  return `
    <table border="0" cellpadding="0" cellspacing="0" width="100%"
           style="margin:20px 0;background:white;border-radius:12px;
                  border:1px solid #e0e0e0;font-family:Arial,sans-serif;">
      <tr><td style="padding:20px;">
        <div style="margin-bottom:20px;">
          <h3 style="margin:0;color:#333;font-size:18px;font-weight:600;">${title}</h3>
          ${subtitle ? `<p style="margin:5px 0 0;color:#666;font-size:14px;">${subtitle}</p>` : ''}
          ${unit ? `<p style="margin:5px 0 0;color:#888;font-size:12px;">Unit: ${unit} | Frequency: ${frequency || 'Monthly'}</p>` : ''}
        </div>

        ${directionGuideHtml}
        ${statsBox}
        ${thresholdFocusHtml}

        <table border="0" cellpadding="0" cellspacing="0" width="100%">
          <tr>
            <td width="60%" valign="top" style="padding-right:20px;">
              ${preChartLimitsHtml}
              <table border="0" cellpadding="0" cellspacing="0" width="100%">
                <tr>
  
                  <td valign="top" style="padding-left:5px;">
                    <table border="0" cellpadding="0" cellspacing="0" width="100%" style="text-align:center;">
                      <tr>
                        <td align="center">
                          <img src="${chartUrl}"
                               width="500"
                               height="${chartHeightPx}"
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

              <!--High/Low limits beside each other -->
              ${postChartLimitsHtml}
            </td>

            <td width="40%" valign="top" style="padding-left:20px;border-left:2px solid #f0f0f0;">
              ${correctiveActionsHtml}
              ${commentsHtml}

              ${comments.length === 0 && correctiveActions.length === 0 ? `
                <div style="background:#f8f9fa;border-radius:12px;padding:30px;
                            text-align:center;border:1px dashed #e0e0e0;">
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
               k.subject, k.indicator_sub_title, k.unit, k.target, k.target_direction, k.min, k.max,
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

    // â”€â”€ Pass 1: build kpisData entries and accumulate into monthly maps â”€â”€â”€â”€â”€â”€
    histRes.rows.forEach(row => {
      const kpiId = row.kpi_id;
      if (!kpisData[kpiId]) {
        kpisData[kpiId] = {
          title: row.subject,
          subtitle: row.indicator_sub_title || '',
          unit: row.unit || '',
          target: row.target,
          target_direction: row.target_direction,
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

    // â”€â”€ Pass 2: convert monthly maps â†’ sorted month labels + averaged values â”€
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

    // â”€â”€ Sorted month labels for all KPIs â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const weekLabels = Array.from(monthLabelsSet).sort((a, b) => new Date(a) - new Date(b));
    if (weekLabels.length === 0) return null;

    // â”€â”€ Build chart objects â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

      // Labels are already "Jan 2026", "Feb 2026" etc â€” pass through as-is
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

    // â”€â”€ Build charts using CURRENT (old) target from Kpi table â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const chartsData = await generateWeeklyReportData(responsibleId, reportWeek);
    let chartsHtml = '';

    if (chartsData && chartsData.length > 0) {
      chartsData.forEach(chart => { chartsHtml += generateVerticalBarChart(chart); });
    } else {
      chartsHtml = `
        <div style="text-align:center;padding:60px;background:#f8f9fa;border-radius:12px;">
          <p style="color:#495057;margin:0;font-size:18px;">No KPI Data Available</p>
        </div>`;
    }

    const emailHtml = `<!DOCTYPE html><html><head><meta charset="utf-8"></head>
    <body style="margin:0;padding:0;font-family:Arial,sans-serif;background:#f4f6f9;">
      <table border="0" cellpadding="0" cellspacing="0" width="100%" style="background:#f4f6f9;">
        <tr><td align="center" style="padding:20px;">
          <table border="0" cellpadding="0" cellspacing="0" width="100%">
            <tr><td style="background:#0078D7;padding:30px;text-align:center;border-radius:8px 8px 0 0;">
              <h1 style="margin:0;color:white;font-size:24px;">KPI Performance Report</h1>
              <p style="margin:10px 0 20px;color:rgba(255,255,255,0.9);">
                ${reportWeek.replace('2026-Week', 'Week ')} | ${responsible.name} | ${responsible.plant_name}
              </p>
              <table border="0" cellpadding="0" cellspacing="0" align="center"><tr>
                <td style="padding:0 8px;">
                  <a href="https://kpi-codir.azurewebsites.net/kpi-trends?responsible_id=${responsible.responsible_id}"
                     style="display:inline-block;padding:12px 24px;background:#38bdf8;color:white;
                            text-decoration:none;border-radius:8px;font-weight:600;font-size:14px;">
                    View KPI Graphics</a>
                </td>
                <td style="padding:0 8px;">
                  <a href="https://kpi-codir.azurewebsites.net/dashboard?responsible_id=${responsible.responsible_id}"
                     style="display:inline-block;padding:12px 24px;background:#38bdf8;color:white;
                            text-decoration:none;border-radius:8px;font-weight:600;font-size:14px;">
                    View Dashboard</a>
                </td>
              </tr></table>
            </td></tr>

            <tr><td style="padding:20px 30px 0;">
              <div style="background:#fff8e1;border:1px solid #ffe082;border-radius:8px;padding:14px 18px;">
                <span style="font-size:14px;color:#5f4200;">
                  <strong>AI Recommendations PDF is attached</strong> - open it for root-cause analysis,
                  action plans and improvement roadmaps for each KPI.
                </span>
              </div>
            </td></tr>

            <tr><td style="padding:16px 30px 0;">
              <div style="background:#f8fbff;border:1px solid #dbeafe;border-radius:10px;padding:16px 18px;">
                <div style="font-size:12px;font-weight:700;color:#0f6cbd;text-transform:uppercase;
                            letter-spacing:0.08em;margin-bottom:8px;">Good Direction Guide</div>
                <div style="font-size:14px;color:#1f2937;line-height:1.6;">
                  <strong>Up</strong> = higher is better, for example Sales and OTD.<br />
                  <strong>Down</strong> = lower is better, for example Scrap, accidents, and customer claims.<br />
                  This direction is used for colors, alerts, and target management across the report.
                </div>
              </div>
            </td></tr>

            <tr><td style="padding:30px;">${chartsHtml}</td></tr>

            <tr><td style="padding:20px;background:#f8f9fa;border-top:1px solid #e9ecef;
                            text-align:center;font-size:12px;color:#666;">
              AVOCarbon KPI System | Generated ${new Date().toLocaleDateString('en-GB')}
            </td></tr>
          </table>
        </td></tr>
      </table>
    </body></html>`;

    // â”€â”€ Generate PDF attachment â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    let pdfAttachment = null;
    try {
      console.log(`ðŸ“„ Generating recommendations PDF for ${responsible.name}â€¦`);
      const pdfBuffer = await generateKPIRecommendationsPDFBuffer(pool, responsibleId, reportWeek);
      if (pdfBuffer) {
        const weekLabel = reportWeek.replace('2026-Week', 'Week_');
        pdfAttachment = {
          filename: `KPI_Recommendations_${responsible.name.replace(/ /g, '_')}_${weekLabel}.pdf`,
          content: pdfBuffer,
          contentType: 'application/pdf',
        };
        console.log(`ðŸ“„ PDF ready â€” ${(pdfBuffer.length / 1024).toFixed(1)} KB`);
      }
    } catch (pdfErr) {
      console.error(`âš ï¸ PDF generation failed for ${responsible.name}:`, pdfErr.message);
    }

    // â”€â”€ SEND EMAIL (with OLD target values in charts) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const transporter = createTransporter();
    await transporter.sendMail({
      from: '"AVOCarbon KPI System" <administration.STS@avocarbon.com>',
      to: responsible.email,
      subject: `KPI Performance Trends - ${reportWeek} | ${responsible.name}`,
      html: emailHtml,
      attachments: pdfAttachment ? [pdfAttachment] : [],
    });
    console.log(`Email sent to ${responsible.email}`);

    // â”€â”€ NOW apply all pending target updates for this responsible â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    // Email is already sent â€” safe to update Kpi.target and hist26.target
    try {
      const pending = await pool.query(
        `SELECT p.id, p.kpi_id, p.week, p.new_target,
                k.target AS current_kpi_target, k.subject
         FROM public.pending_target_updates p
         JOIN public."Kpi" k ON k.kpi_id = p.kpi_id
         WHERE p.responsible_id = $1 AND p.applied = false`,
        [responsibleId]
      );

      console.log(`ðŸ“‹ ${pending.rows.length} pending target update(s) to apply for ${responsible.name}`);

      for (const row of pending.rows) {
        const newVal = parseFloat(row.new_target);
        const currVal = parseFloat(row.current_kpi_target);

        if (isNaN(newVal)) {
          console.warn(`âš ï¸ Skipping KPI ${row.kpi_id} â€” new_target "${row.new_target}" is not a number`);
          continue;
        }

        // 1. Update Kpi.target
        await pool.query(
          `UPDATE public."Kpi" SET target = $1 WHERE kpi_id = $2`,
          [String(newVal), row.kpi_id]
        );
        console.log(`ðŸŽ¯ Kpi.target updated: "${row.subject}" (${row.kpi_id}) ${currVal} â†’ ${newVal}`);

        // 2. Update kpi_values_hist26.target for that week
        await pool.query(
          `UPDATE public.kpi_values_hist26
           SET target = $1
           WHERE responsible_id = $2 AND kpi_id = $3 AND week = $4`,
          [newVal, responsibleId, row.kpi_id, row.week]
        );
        console.log(`ðŸ“ kpi_values_hist26.target updated: KPI ${row.kpi_id} week ${row.week} â†’ ${newVal}`);

        // 3. Mark as applied
        await pool.query(
          `UPDATE public.pending_target_updates SET applied = true WHERE id = $1`,
          [row.id]
        );
      }

      console.log(`All pending target updates applied for ${responsible.name}`);

    } catch (applyErr) {
      console.error(`âŒ Failed to apply pending target updates for ${responsible.name}:`, applyErr.message);
    }

  } catch (error) {
    console.error(`âŒ generateWeeklyReportEmail failed for responsible ${responsibleId}:`, error.message);
    throw error;
  }
};
// ---------- Cron: weekly KPI submission email ----------
// let cronRunning = false;
// cron.schedule("17 11 * * *", async () => {
//   const lockId = "send_kpi_weekly_email_job";
//   const lock = await acquireJobLock(lockId);
//   if (!lock.acquired) return;
//   try {
//     if (cronRunning) return;
//     cronRunning = true;
//     const now = new Date();
//     const startOfYear = new Date(now.getFullYear(), 0, 1);
//     const dayOfYear = Math.floor((now - startOfYear) / (24 * 60 * 60 * 1000));
//     const currentWeek = Math.ceil((dayOfYear + startOfYear.getDay() + 1) / 7);
//     const forcedWeek = `${now.getFullYear()}-Week${currentWeek}`;
//     const resps = await pool.query(
//       `SELECT DISTINCT r.responsible_id FROM public."Responsible" r
//        JOIN public.kpi_values kv ON kv.responsible_id = r.responsible_id WHERE kv.week = $1`,
//       [forcedWeek]
//     );
//     for (let r of resps.rows) await sendKPIEmail(r.responsible_id, forcedWeek);
//     console.log(`KPI emails sent to ${resps.rows.length} responsibles`);
//   } catch (err) {
//     console.error("Scheduled email error:", err.message);
//   } finally {
//     cronRunning = false;
//     await releaseJobLock(lockId, lock.instanceId, lock.lockHash);
//   }
// }, { scheduled: true, timezone: "Africa/Tunis" });


// ---------- Cron: weekly reports ----------
// let reportCronRunning = false;
// cron.schedule("21 17 * * *", async () => {
//   const lockId = "weekly_kpi_report_job";
//   const lock = await acquireJobLock(lockId);
//   if (!lock.acquired) return;
//   try {
//     if (reportCronRunning) return;
//     reportCronRunning = true;
//     const now = new Date();
//     const year = now.getFullYear();
//     const getWeekNumber = (date) => {
//       const d = new Date(date); d.setHours(0, 0, 0, 0);
//       d.setDate(d.getDate() + 4 - (d.getDay() || 7));
//       const yearStart = new Date(d.getFullYear(), 0, 1);
//       return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
//     };
//     const weekNumber = getWeekNumber(now);
//     const previousWeek = `${year}-Week${weekNumber - 1}`;
//     const resps = await pool.query(
//       `SELECT DISTINCT r.responsible_id, r.email, r.name
//        FROM public."Responsible" r JOIN public.kpi_values_hist26 h ON r.responsible_id = h.responsible_id
//        WHERE r.email IS NOT NULL AND r.email != ''
//        GROUP BY r.responsible_id, r.email, r.name HAVING COUNT(h.hist_id) > 0`
//     );
//     for (const [index, resp] of resps.rows.entries()) {
//       try {
//         await generateWeeklyReportEmail(resp.responsible_id, previousWeek);
//         await new Promise(resolve => setTimeout(resolve, 1500));
//       } catch (err) {
//         console.error(`Failed for ${resp.name}:`, err.message);
//       }
//     }
//     console.log(`Weekly reports sent`);
//   } catch (error) {
//     console.error("Report cron error:", error.message);
//   } finally {
//     reportCronRunning = false;
//     await releaseJobLock(lockId, lock.instanceId, lock.lockHash);
//   }
// }, { scheduled: true, timezone: "Africa/Tunis" });

// ============================================================
// createIndividualKPIChart
// ============================================================
const createIndividualKPIChart = (kpi) => {
  const target = kpi.target && kpi.target !== 'None' ? Number(kpi.target) : null;
  const high_limit = kpi.high_limit && kpi.high_limit !== 'None' ? Number(kpi.high_limit) : null;
  const low_limit = kpi.low_limit && kpi.low_limit !== 'None' ? Number(kpi.low_limit) : null;
  const direction = inferKpiDirection(kpi);
  const thresholdLineValues = getReadableThresholdLineValues(low_limit, target, high_limit);
  const displayHigh = thresholdLineValues.displayHigh;
  const displayLow = thresholdLineValues.displayLow;
  const displayTarget = thresholdLineValues.displayTarget;

  const weeklyData = kpi.weeklyData || { weeks: [], values: [] };
  const weeks = weeklyData.weeks.slice(0, 12);
  const values = weeklyData.values.slice(0, 12);

  if (!values || values.length === 0) {
    return `<table border="0" cellpadding="15" cellspacing="0" width="100%"
              style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;margin-bottom:15px;">
      <tr><td style="text-align:center;color:#999;font-size:14px;padding:20px;">
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
    ? (direction === 'down' ? '\u2197' : '\u2198')
    : (direction === 'down' ? '\u2198' : '\u2197');



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
  const axisRange = getAutoChartAxisRange(values, displayLow, displayTarget, displayHigh);

  if (high_limit !== null) {
    datasets.push({
      label: `High Limit (${fmt(high_limit)})`,
      data: new Array(values.length).fill(displayHigh),
      borderColor: '#f97316', borderWidth: 2, borderDash: [6, 4],
      lineTension: 0, pointRadius: 0, fill: false
    });
  }

  if (target !== null) {
    datasets.push({
      label: `Target (${fmt(target)})`,
      data: new Array(values.length).fill(displayTarget),
      borderColor: '#16a34a', borderWidth: 2.25,
      lineTension: 0, pointRadius: 0, fill: false
    });
  }

  if (low_limit !== null) {
    datasets.push({
      label: `Low Limit (${fmt(low_limit)})`,
      data: new Array(values.length).fill(displayLow),
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
        yAxes: [{
          ticks: {
            fontSize: 10,
            beginAtZero: false,
            min: axisRange.min,
            max: axisRange.max
          },
          gridLines: { color: 'rgba(0,0,0,0.05)' }
        }]
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
      ? `<div style="background:#e8f5e9;color:#2e7d32;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #a5d6a7;display:inline-block;">Target: ${target}</div>`
      : `<div style="background:#f5f5f5;color:#9e9e9e;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #e0e0e0;display:inline-block;">Target: N/A</div>`}</td>
        <td align="center" width="33%">${high_limit !== null
      ? `<div style="background:#fff3e0;color:#e65100;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #ffb74d;display:inline-block;">High Limit: ${high_limit}</div>`
      : `<div style="background:#f5f5f5;color:#9e9e9e;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #e0e0e0;display:inline-block;">High Limit: N/A</div>`}</td>
        <td align="center" width="33%">${low_limit !== null
      ? `<div style="background:#ffebee;color:#c62828;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #ef5350;display:inline-block;">Low Limit: ${low_limit}</div>`
      : `<div style="background:#f5f5f5;color:#9e9e9e;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #e0e0e0;display:inline-block;">Low Limit: N/A</div>`}</td>
      </tr>
    </table>`;
  const thresholdReadabilityNote = thresholdLineValues.hasTightCluster ? `
    <div style="margin-top:8px;text-align:center;font-size:10px;color:#64748b;line-height:1.4;">
      Threshold lines are slightly spaced in the chart for readability. Exact values are shown above.
    </div>
  ` : '';

  const hasComments = kpi.comments && kpi.comments.length > 0;
  const correctiveActionDueDate = kpi.correctiveAction?.dueDate
    ? formatInputDate(kpi.correctiveAction.dueDate)
    : '';
  const correctiveActionWeekLabel = kpi.correctiveAction?.week
    ? weekToMonthLabel(kpi.correctiveAction.week) || String(kpi.correctiveAction.week).replace('2026-Week', 'Week ')
    : '';
  const hasCA = kpi.correctiveAction && (
    kpi.correctiveAction.rootCause ||
    kpi.correctiveAction.implementedSolution ||
    correctiveActionDueDate
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
          COMMENTS
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
          CORRECTIVE ACTION ${caStatusBadge}
        </div>

        ${correctiveActionWeekLabel ? `
        <div style="font-size:10px;font-weight:700;color:#64748b;text-transform:uppercase;
                    letter-spacing:0.08em;margin:0 0 8px;">
          Action Week: ${correctiveActionWeekLabel}
        </div>` : ''}

        ${kpi.correctiveAction.rootCause ? `
        <table border="0" cellpadding="0" cellspacing="0" width="100%"
               style="margin-bottom:8px;">
          <tr><td style="padding:8px 10px;background:#fff5f5;border-radius:6px;
                         border-left:3px solid #ef4444;">
            <div style="font-size:9px;font-weight:800;color:#dc2626;
                        text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">
              Root Cause
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
              Implemented Solution
            </div>
            <div style="font-size:11px;color:#374151;line-height:1.5;">
              ${kpi.correctiveAction.implementedSolution}
            </div>
          </td></tr>
        </table>` : ''}

        ${correctiveActionDueDate ? `
        <table border="0" cellpadding="0" cellspacing="0" width="100%"
               style="margin-bottom:8px;">
          <tr><td style="padding:8px 10px;background:#f5f3ff;border-radius:6px;
                         border-left:3px solid #7c3aed;">
            <div style="font-size:9px;font-weight:800;color:#6d28d9;
                        text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">
              Due Date
            </div>
            <div style="font-size:11px;color:#374151;line-height:1.5;">
              ${correctiveActionDueDate}
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
          ${thresholdReadabilityNote}
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
      <h1 style="margin:0 0 8px;font-size:28px;font-weight:800;color:#2c3e50;">CEO KPI CODIR DASHBOARD</h1>
      <div style="font-size:14px;color:#6c757d;">
        <strong>${plant.plant_name}</strong>  Week: <strong>${week.replace('2026-Week', 'W')}</strong>
        Manager: <strong>${plant.manager || 'N/A'}</strong></div>
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
            <h1 style="color:#2c3e50;font-size:28px;">No KPI Trends Available</h1>
            <p style="color:#666;font-size:16px;">Start filling KPI forms to see trend charts.</p>
            <a href="/form?responsible_id=${responsible_id}&week=${getCurrentWeek()}"
               style="display:inline-block;margin-top:20px;padding:15px 30px;
                      background:linear-gradient(135deg,#667eea,#764ba2);color:white;
                      text-decoration:none;border-radius:12px;font-weight:600;">
              Start Filling KPIs</a>
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
      kpi.trendIcon = cur > prev ? '\u2197' : cur < prev ? '\u2198' : '\u2192';
      kpi.trendColor = cur > prev ? '#10b981' : cur < prev ? '#ef4444' : '#6b7280';
    } else { kpi.trend = 0; kpi.trendIcon = '\u2192'; kpi.trendColor = '#6b7280'; }
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
      <h1 style="font-size:36px;font-weight:800;color:#fff;margin-bottom:10px;">KPI Trends & Analytics</h1>
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
      <p style="color:rgba(255,255,255,0.8);">AVOCarbon Industrial Analytics | ${new Date().getFullYear()}</p>
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
            font-weight:600;background:#f3f4f6;color:#374151;">Unit: ${kpi.unit}</span>` : ''}
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
            <span>Target</span>
          </div>
          <div style="font-size:24px;font-weight:700;color:#f97316;">
            ${kpi.target !== null ? kpi.target.toFixed(2) : 'N/A'}
          </div>
        </div>
        
        <!-- High Limit Box -->
        <div style="background:#f9fafb;border-radius:12px;padding:15px;text-align:center;border:1px solid #e5e7eb;">
          <div style="font-size:12px;color:#6b7280;margin-bottom:5px;text-transform:uppercase;display:flex;align-items:center;justify-content:center;gap:4px;">
            <span>High Limit</span>
          </div>
          <div style="font-size:24px;font-weight:700;color:#f97316;">
            ${kpi.high_limit !== null ? kpi.high_limit.toFixed(2) : 'N/A'}
          </div>
        </div>
        
        <!-- Low Limit Box -->
        <div style="background:#f9fafb;border-radius:12px;padding:15px;text-align:center;border:1px solid #e5e7eb;">
          <div style="font-size:12px;color:#6b7280;margin-bottom:5px;text-transform:uppercase;display:flex;align-items:center;justify-content:center;gap:4px;">
            <span>Low Limit</span>
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
          <div style="font-size:12px;color:#64748b;margin-bottom:5px;">Definition</div>
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
  const axisRange = getAutoChartAxisRange(kpi.values, lowLimit, null, highLimit);

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
          y: { beginAtZero: false, min: ${axisRange.min}, max: ${axisRange.max}, grid: { color: 'rgba(0,0,0,0.05)' },
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
        <div style="font-size:72px;color:#ef4444;margin-bottom:30px;">âŒ</div>
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
              ca.root_cause, ca.implemented_solution, ca.evidence, ca.status AS ca_status,
              ca.due_date AS ca_due_date, ca.week AS ca_week
       FROM LatestKPIValues lkv
       LEFT JOIN LATERAL (
         SELECT root_cause, implemented_solution, evidence, status, due_date, week
         FROM public.corrective_actions
         WHERE kpi_id = lkv.kpi_id
           AND responsible_id = lkv.responsible_id
         ORDER BY
           CASE
             WHEN week = lkv.week
              AND (
                NULLIF(BTRIM(COALESCE(root_cause, '')), '') IS NOT NULL
                OR NULLIF(BTRIM(COALESCE(implemented_solution, '')), '') IS NOT NULL
                OR NULLIF(BTRIM(COALESCE(evidence, '')), '') IS NOT NULL
                OR due_date IS NOT NULL
              ) THEN 0
             WHEN
                NULLIF(BTRIM(COALESCE(root_cause, '')), '') IS NOT NULL
                OR NULLIF(BTRIM(COALESCE(implemented_solution, '')), '') IS NOT NULL
                OR NULLIF(BTRIM(COALESCE(evidence, '')), '') IS NOT NULL
                OR due_date IS NOT NULL THEN 1
             WHEN week = lkv.week THEN 2
             ELSE 3
           END,
           COALESCE(updated_date, created_date) DESC,
           corrective_action_id DESC
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

    // Convert monthly map â†’ sorted arrays
    Object.values(weeklyDataByKPI).forEach(kpi => {
      const mm = kpi._monthlyMap || {};
      const sortedMonths = Object.keys(mm).sort((a, b) => new Date(a) - new Date(b));
      kpi.weeks = sortedMonths;                                        // ["Jan 2026", "Feb 2026", â€¦]
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
      if (!existing.correctiveAction && (row.root_cause || row.implemented_solution || row.evidence || row.ca_due_date)) {
        existing.correctiveAction = {
          rootCause: (row.root_cause || '').trim(),
          implementedSolution: (row.implemented_solution || '').trim(),
          evidence: (row.evidence || '').trim(),
          dueDate: row.ca_due_date || null,
          week: row.ca_week || null,
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
    const reportWeek = currentWeek;
    const reportData = await getDepartmentKPIReport(plantId, reportWeek);
    if (!reportData || reportData.stats.totalKPIs === 0) return null;

    const emailHtml = generateManagerReportHtml(reportData);

    // â”€â”€ Generate plant-wide recommendations PDF â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    let pdfAttachment = null;
    try {
      console.log(`ðŸ“„ Generating plant-wide recommendations PDF for plant=${plantId}â€¦`);
      const pdfBuffer = await generatePlantKPIRecommendationsPDFBuffer(pool, plantId, reportWeek);
      if (pdfBuffer) {
        const weekLabel = reportWeek.replace('2026-Week', 'Week_');
        pdfAttachment = {
          filename: `KPI_Recommendations_${reportData.plant.plant_name.replace(/ /g, '_')}_${weekLabel}.pdf`,
          content: pdfBuffer,
          contentType: 'application/pdf',
        };
        console.log(`ðŸ“„ Plant PDF ready â€” ${(pdfBuffer.length / 1024).toFixed(1)} KB`);
      }
    } catch (pdfErr) {
      // Never block the main email if PDF generation fails
      console.error(`Could not generate plant recommendations PDF:`, pdfErr.message);
    }

    // â”€â”€ Send email with optional PDF attachment â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const transporter = createTransporter();
    await transporter.sendMail({
      from: '"AVOCarbon Plant Analytics" <administration.STS@avocarbon.com>',
      to: reportData.plant.manager_email,
      subject: `Weekly KPI Dashboard - ${reportData.plant.plant_name} - Week ${reportWeek.replace('2026-Week', '')}`,
      html: emailHtml,
      attachments: pdfAttachment ? [pdfAttachment] : [],
    });

    console.log(`KPI report${pdfAttachment ? ' + recommendations PDF' : ''} sent to ${reportData.plant.manager_email}`);
  } catch (error) {
    console.error(`Failed to send report for plant ${plantId}:`, error.message);
  }
};

// ---------- Cron: corrective action escalation reminders ----------
let correctiveActionEscalationCronRunning = false;
cron.schedule("35 9 * * *", async () => {
  const lockId = "corrective_action_escalation_job";
  const lock = await acquireJobLock(lockId);
  if (!lock.acquired) return;
  try {
    if (correctiveActionEscalationCronRunning) return;
    correctiveActionEscalationCronRunning = true;
    const result = await runCorrectiveActionEscalationJob();
    console.log(
      `[Corrective Action Escalation] Processed ${result.pending} pending escalation(s); sent ${result.sent} email(s).`
    );
  } catch (error) {
    console.error("[Corrective Action Escalation] Cron error:", error.message);
  } finally {
    correctiveActionEscalationCronRunning = false;
    await releaseJobLock(lockId, lock.instanceId, lock.lockHash);
  }
}, { scheduled: true, timezone: "Africa/Tunis" });

// ---------- Cron: weekly manager/plant report ----------
// let managerCronRunning = false;
// cron.schedule("18 17 * * *", async () => {
//   const lockId = "department_report_job";
//   const lock = await acquireJobLock(lockId);
//   if (!lock.acquired) return;
//   try {
//     if (managerCronRunning) return;
//     managerCronRunning = true;
//     const now = new Date();
//     const year = now.getFullYear();
//     const getWeekNumber = (date) => {
//       const d = new Date(date); d.setHours(0, 0, 0, 0);
//       d.setDate(d.getDate() + 4 - (d.getDay() || 7));
//       const yearStart = new Date(d.getFullYear(), 0, 1);
//       return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
//     };
//     const weekNumber = getWeekNumber(now);
//     const currentWeek = `${year}-Week${weekNumber}`;
//     console.log(`[Manager Report] Sending reports for week ${currentWeek}...`);
//     const plantsRes = await pool.query(
//       `SELECT plant_id, name, manager_email FROM public."Plant"
//        WHERE manager_email IS NOT NULL AND manager_email != ''`
//     );
//     console.log(`ðŸ“‹ Found ${plantsRes.rows.length} plants with manager emails`);
//     for (const plant of plantsRes.rows) {
//       try {
//         await sendDepartmentKPIReportEmail(plant.plant_id, currentWeek);
//         console.log(`Report sent for plant: ${plant.name}`);
//         await new Promise(resolve => setTimeout(resolve, 1500));
//       } catch (err) {
//         console.error(`  âŒ Failed for plant ${plant.name}:`, err.message);
//       }
//     }
//     console.log(`[Manager Report] All plant reports sent`);
//   } catch (error) {
//     console.error("âŒ [Manager Report] Cron error:", error.message);
//   } finally {
//     managerCronRunning = false;
//     await releaseJobLock(lockId, lock.instanceId, lock.lockHash);
//   }
// }, { scheduled: true, timezone: "Africa/Tunis" });


registerRecommendationRoutes(app, pool, createTransporter);
ensureCorrectiveActionEscalationSchema()
  .then(() => {
    console.log("[Corrective Action Escalation] Tracking table is ready.");
  })
  .catch((error) => {
    console.error("[Corrective Action Escalation] Tracking table setup failed:", error.message);
  });
// ---------- Start server ----------
app.listen(port, () => console.log(`ðŸš€ Server running on port ${port}`));
