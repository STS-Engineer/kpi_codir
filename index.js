require("dotenv").config();
const express = require("express");
const { Pool } = require("pg");
const bodyParser = require("body-parser");
const cors = require("cors");
const nodemailer = require("nodemailer");
const cron = require("node-cron");
const { registerRecommendationRoutes } = require('./kpi-recommendations');
const { generateKPIRecommendationsPDFBuffer, generatePlantKPIRecommendationsPDFBuffer } = require('./kpi-recommendations');
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


cron.schedule('00 07 * * 1', async () => {
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
// DOT COLOR HELPER
// red    = below low_limit
// orange = within 10% above low_limit (close to it)
// green  = fine
// ============================================================
const getDotColor = (value, lowLimit, highLimit) => {
  const val = parseFloat(value);
  if (isNaN(val)) return '#6c757d';
  const low = (lowLimit !== null && lowLimit !== undefined &&
    lowLimit !== '' && lowLimit !== 'None')
    ? parseFloat(lowLimit) : null;
  if (low !== null) {
    if (val < low) return '#dc3545';        // red
    if (val < low * 1.10) return '#ff9800'; // orange
  }
  return '#28a745'; // green
};

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

const releaseJobLock = async (lockId, instanceId, lockHash) => {
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
            ORDER BY h.updated_at DESC LIMIT 1) as latest_comment,
           -- Corrective action fields from corrective_actions table
           ca.corrective_action_id,
           ca.root_cause       AS ca_root_cause,
           ca.implemented_solution AS ca_implemented_solution,
           ca.evidence         AS ca_evidence,
           ca.status           AS ca_status
    FROM public.kpi_values kv
    JOIN "Kpi" k ON kv.kpi_id = k.kpi_id
    LEFT JOIN public.corrective_actions ca
           ON ca.kpi_id = kv.kpi_id
          AND ca.responsible_id = $1
          AND ca.week = $2
    WHERE kv.responsible_id = $1 AND kv.week = $2
    ORDER BY k.kpi_id ASC`,
    [responsibleId, week]
  );
  return { responsible, kpis: kpiRes.rows };
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
    const kpiRes = await pool.query(`SELECT target FROM public."Kpi" WHERE kpi_id = $1`, [kpiId]);
    if (!kpiRes.rows.length) return { targetUpdated: false };
    const currentTarget = parseFloat(kpiRes.rows[0].target);
    const numValue = parseFloat(newValue);
    if (isNaN(numValue) || isNaN(currentTarget)) return { targetUpdated: false };

    await pool.query(
      `UPDATE public.kpi_values_hist26
       SET target = $1
       WHERE responsible_id = $2 AND kpi_id = $3 AND week = $4 AND (target IS NULL OR target < $1)`,
      [numValue, responsibleId, kpiId, week]
    );

    if (numValue > currentTarget) {
      await pool.query(`UPDATE public."Kpi" SET target = $1 WHERE kpi_id = $2`, [numValue, kpiId]);
      return { targetUpdated: true, updateInfo: { kpiId, oldTarget: currentTarget, newTarget: numValue } };
    }

    return { targetUpdated: false };
  } catch (error) {
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
  { rootCause, implementedSolution, evidence }
) => {
  try {
    const result = await pool.query(
      `UPDATE public.corrective_actions
       SET root_cause = $4::text,
           implemented_solution = $5::text,
           evidence = $6::text,
           status = CASE
             WHEN $4::text IS NOT NULL
              AND $5::text IS NOT NULL
              AND $6::text IS NOT NULL
             THEN 'Waiting for validation'
             ELSE status
           END,
           updated_date = NOW()
       WHERE responsible_id = $1
       AND kpi_id = $2
       AND week = $3
       RETURNING corrective_action_id`,
      [
        responsibleId,
        kpiId,
        week,
        rootCause || null,
        implementedSolution || null,
        evidence || null
      ]
    );

    // If no row was updated → insert
    if (result.rowCount === 0) {
      await pool.query(
        `INSERT INTO public.corrective_actions
         (responsible_id, kpi_id, week, root_cause, implemented_solution, evidence, status)
         VALUES ($1,$2,$3,$4::text,$5::text,$6::text,
           CASE
             WHEN $4::text IS NOT NULL
              AND $5::text IS NOT NULL
              AND $6::text IS NOT NULL
             THEN 'Waiting for validation'
             ELSE 'Open'
           END)`,
        [
          responsibleId,
          kpiId,
          week,
          rootCause || null,
          implementedSolution || null,
          evidence || null
        ]
      );
    }

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
    const OpenAI = require("openai");
    const openai = new OpenAI({ apiKey: process.env.SECRET_KEY });

    const currentVal = parseFloat(kpi.value || 0);
    const targetVal = parseFloat(kpi.target || 0);
    const gap = targetVal > 0 ? (targetVal - currentVal).toFixed(2) : "N/A";
    const pctGap = targetVal > 0
      ? (((targetVal - currentVal) / targetVal) * 100).toFixed(1)
      : "N/A";

    const prompt = `You are an industrial manufacturing and continuous-improvement expert.

A KPI is BELOW its target. Give exactly 2 distinct corrective action plans.
Each plan must have:
1. root_cause       – likely reason the KPI is below target (1-2 sentences)
2. immediate_action – what to do right now, within 1 week (1-2 sentences)
3. evidence         – how to prove the action is working (1-2 sentences)

The 2 plans should represent DIFFERENT root-cause hypotheses so the responsible can pick the most relevant one.

KPI Details:
- Indicator: ${kpi.subject}${kpi.indicator_sub_title ? ` — ${kpi.indicator_sub_title}` : ""}
- Unit: ${kpi.unit || "N/A"}
- Current Value: ${currentVal} ${kpi.unit || ""}
- Target Value: ${targetVal} ${kpi.unit || ""}
- Gap: ${gap} ${kpi.unit || ""} (${pctGap}% below target)

Rules:
- Be specific to this KPI context, not generic
- Return ONLY valid JSON, no markdown, no extra text

Format:
{
  "suggestion_1": { "root_cause": "...", "immediate_action": "...", "evidence": "..." },
  "suggestion_2": { "root_cause": "...", "immediate_action": "...", "evidence": "..." }
}`;

    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [{ role: "user", content: prompt }],
      temperature: 0.7,
      max_tokens: 600,
    });

    const raw = completion.choices[0].message.content.trim()
      .replace(/```json|```/g, "").trim();
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
              k.unit, k.target, kv.value
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

// ========== CORRECTIVE ACTION ROUTES ==========
app.get("/corrective-actions-list", async (req, res) => {
  try {
    const { responsible_id } = req.query;
    const actionsRes = await pool.query(
      `SELECT ca.*, k.indicator_title, k.indicator_sub_title, k.unit
       FROM public.corrective_actions ca
       JOIN public."Kpi" k ON ca.kpi_id = k.kpi_id
       WHERE ca.responsible_id = $1 ORDER BY ca.created_date DESC`,
      [responsible_id]
    );
    const actions = actionsRes.rows;
    res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>Corrective Actions</title>
      <style>body{font-family:'Segoe UI',sans-serif;background:#f4f6f9;padding:20px}
      .container{max-width:1200px;margin:0 auto;background:white;padding:30px;border-radius:8px}
      h1{color:#0078D7}table{width:100%;border-collapse:collapse;margin-top:20px}
      th,td{padding:12px;border:1px solid #ddd}th{background:#0078D7;color:white}
      tr:nth-child(even){background:#f8f9fa}
      .status-open{background:#ffebee;color:#c62828;padding:4px 10px;border-radius:12px;font-size:12px}
      .status-completed{background:#e8f5e9;color:#2e7d32;padding:4px 10px;border-radius:12px;font-size:12px}
      </style></head><body>
      <div class="container"><h1>📋 Corrective Actions History</h1>
      <table><thead><tr><th>Week</th><th>KPI</th><th>Status</th><th>Created</th><th>Action</th></tr></thead>
      <tbody>${actions.length === 0
        ? '<tr><td colspan="5" style="text-align:center;padding:40px;color:#999;">No corrective actions found</td></tr>'
        : actions.map(a => `<tr>
            <td>${a.week}</td>
            <td><strong>${a.indicator_title}</strong>${a.indicator_sub_title ? `<br><small>${a.indicator_sub_title}</small>` : ''}</td>
            <td><span class="status-${a.status.toLowerCase()}">${a.status}</span></td>
            <td>${new Date(a.created_date).toLocaleDateString()}</td>
            <td><a href="/corrective-action-form?responsible_id=${responsible_id}&kpi_id=${a.kpi_id}&week=${a.week}"
                   style="color:#0078D7;font-weight:600;">${a.status === 'Open' ? 'Complete' : 'View'}</a></td>
          </tr>`).join('')}
      </tbody></table></div></body></html>`);
  } catch (err) {
    res.status(500).send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});

app.get("/corrective-action-form", async (req, res) => {
  try {
    const { responsible_id, kpi_id, week } = req.query;
    const resResp = await pool.query(
      `SELECT r.*, p.name AS plant_name, d.name AS department_name
       FROM public."Responsible" r
       JOIN public."Plant" p ON r.plant_id = p.plant_id
       JOIN public."Department" d ON r.department_id = d.department_id
       WHERE r.responsible_id = $1`, [responsible_id]);
    const responsible = resResp.rows[0];
    if (!responsible) return res.status(404).send("Responsible not found");
    const kpiResp = await pool.query(
      `SELECT k.kpi_id, k.indicator_title, k.indicator_sub_title, k.unit, k.target, kv.value
       FROM public."Kpi" k
       LEFT JOIN public.kpi_values kv ON k.kpi_id = kv.kpi_id
       WHERE k.kpi_id = $1 AND kv.responsible_id = $2 AND kv.week = $3`,
      [kpi_id, responsible_id, week]);
    const kpi = kpiResp.rows[0];
    if (!kpi) return res.status(404).send("KPI not found");
    const existingCA = await pool.query(
      `SELECT * FROM public.corrective_actions WHERE responsible_id = $1 AND kpi_id = $2 AND week = $3
       ORDER BY created_date DESC LIMIT 1`, [responsible_id, kpi_id, week]);
    const ed = existingCA.rows[0] || {};
    res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>Corrective Action</title>
      <style>body{font-family:'Segoe UI',sans-serif;background:#f4f6f9;padding:20px}
      .container{max-width:800px;margin:0 auto;background:white;border-radius:8px;overflow:hidden}
      .header{background:linear-gradient(135deg,#d32f2f,#f44336);color:white;padding:25px;text-align:center}
      .form-section{padding:30px}label{display:block;font-weight:600;margin-bottom:8px}
      textarea{width:100%;padding:12px;border:1px solid #ddd;border-radius:4px;min-height:100px;resize:vertical}
      .submit-btn{background:#d32f2f;color:white;border:none;padding:14px 30px;border-radius:6px;
                  font-size:16px;font-weight:600;cursor:pointer;width:100%}</style></head><body>
      <div class="container">
        <div class="header"><h1>Corrective Action Form</h1><div>Week ${week}</div></div>
        <div class="form-section">
          <form action="/submit-corrective-action" method="POST">
            <input type="hidden" name="responsible_id" value="${responsible_id}">
            <input type="hidden" name="kpi_id" value="${kpi_id}">
            <input type="hidden" name="week" value="${week}">
            ${ed.corrective_action_id ? `<input type="hidden" name="corrective_action_id" value="${ed.corrective_action_id}">` : ''}
            <div style="margin-bottom:20px"><label>Root Cause Analysis *</label>
              <textarea name="root_cause" required>${ed.root_cause || ''}</textarea></div>
            <div style="margin-bottom:20px"><label>Implemented Solution *</label>
              <textarea name="implemented_solution" required>${ed.implemented_solution || ''}</textarea></div>
            <div style="margin-bottom:20px"><label>Evidence of Improvement *</label>
              <textarea name="evidence" required>${ed.evidence || ''}</textarea></div>
            <button type="submit" class="submit-btn">Submit Corrective Action</button>
          </form>
        </div>
      </div></body></html>`);
  } catch (err) {
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
          .form-group{margin-bottom:18px;}
          label{display:block;font-size:13px;font-weight:600;color:#374151;margin-bottom:6px;}
          .required{color:#dc2626;margin-left:3px;}
          textarea{width:100%;padding:11px 14px;border:1.5px solid #d1d5db;border-radius:6px;
            font-size:13px;font-family:inherit;resize:vertical;min-height:80px;
            transition:border-color 0.2s;}
          textarea:focus{border-color:#7c3aed;outline:none;
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
          @media(max-width:640px){.ai-suggestion-row{grid-template-columns:1fr;}}
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
      const rootCause = formData[`root_cause_${caId}`];
      const solution = formData[`solution_${caId}`];
      const evidence = formData[`evidence_${caId}`];
      if (rootCause && solution && evidence) {
        await pool.query(
          `UPDATE public.corrective_actions
           SET root_cause=$1, implemented_solution=$2, evidence=$3,
               status='Waiting for validation', updated_date=NOW()
           WHERE corrective_action_id=$4`,
          [rootCause, solution, evidence, caId]
        );
        completedCount++;
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

    // Extract corrective action fields keyed by kpi_values_id
    // Fields: root_cause_{kpi_values_id}, impl_solution_{kpi_values_id}, evidence_{kpi_values_id}
    const rootCauses = Object.entries(values)
      .filter(([k]) => k.startsWith("root_cause_"))
      .reduce((acc, [k, v]) => { acc[k.replace("root_cause_", "")] = v; return acc; }, {});

    const implSolutions = Object.entries(values)
      .filter(([k]) => k.startsWith("impl_solution_"))
      .reduce((acc, [k, v]) => { acc[k.replace("impl_solution_", "")] = v; return acc; }, {});

    const evidences = Object.entries(values)
      .filter(([k]) => k.startsWith("evidence_"))
      .reduce((acc, [k, v]) => { acc[k.replace("evidence_", "")] = v; return acc; }, {});

    const targetUpdates = [];
    let correctiveActionsCount = 0;

    for (let item of kpiValues) {
      const oldRes = await pool.query(
        `SELECT value, kpi_id FROM public."kpi_values" WHERE kpi_values_id = $1`,
        [item.kpi_values_id]
      );
      if (!oldRes.rows.length) continue;

      const { value: old_value, kpi_id } = oldRes.rows[0];

      // Fetch kpi low_limit to determine if corrective action is needed
      const kpiInfoRes = await pool.query(
        `SELECT low_limit FROM public."Kpi" WHERE kpi_id = $1`, [kpi_id]
      );
      const kpiInfo = kpiInfoRes.rows[0];
      const lowLimit = kpiInfo && kpiInfo.low_limit !== null ? parseFloat(kpiInfo.low_limit) : null;
      const numValue = parseFloat(item.value);

      const rc = rootCauses[item.kpi_values_id] || '';
      const is_ = implSolutions[item.kpi_values_id] || '';
      const ev = evidences[item.kpi_values_id] || '';

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
      // Save corrective action if value is below low_limit AND
      // the user filled in the corrective action fields
      // -------------------------------------------------------
      if (lowLimit !== null && !isNaN(numValue) && numValue < lowLimit) {
        if (rc || is_ || ev) {
          await upsertCorrectiveAction(responsible_id, kpi_id, week, {
            rootCause: rc || null,
            implementedSolution: is_ || null,
            evidence: ev || null,
          });
          correctiveActionsCount++;
        } else {
          // Create an Open corrective action record (without content) if none exists
          const existing = await pool.query(
            `SELECT corrective_action_id FROM public.corrective_actions
             WHERE responsible_id = $1 AND kpi_id = $2 AND week = $3 LIMIT 1`,
            [responsible_id, kpi_id, week]
          );
          if (existing.rows.length === 0) {
            await pool.query(
              `INSERT INTO public.corrective_actions (responsible_id, kpi_id, week, status)
               VALUES ($1, $2, $3, 'Open')`,
              [responsible_id, kpi_id, week]
            );
          }
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
        <a href="/kpi-trends?responsible_id=${responsible_id}" class="btn">View Trends</a>
      </div></body></html>`);
  } catch (err) {
    res.status(500).send(`<h2 style="color:red;">❌ Failed: ${err.message}</h2>`);
  }
});

// ---------- Form page ----------
app.get("/form", async (req, res) => {
  try {
    const { responsible_id, week } = req.query;
    const { responsible, kpis } = await getResponsibleWithKPIs(responsible_id, week);
    if (!kpis.length) return res.send("<p>No KPIs found for this week.</p>");

    let kpiCardsHtml = '';
    kpis.forEach(kpi => {
      let lowLimit = null;
      if (kpi.low_limit && kpi.low_limit !== 'None' && kpi.low_limit !== 'null' &&
        kpi.low_limit !== '' && !isNaN(parseFloat(kpi.low_limit)))
        lowLimit = parseFloat(kpi.low_limit);

      const currentValue = kpi.value && kpi.value !== '' ? parseFloat(kpi.value) : null;
      const showCA = lowLimit !== null && currentValue !== null && currentValue < lowLimit;

      // Pre-fill from existing corrective action if available
      const existingRC = kpi.ca_root_cause || '';
      const existingIS = kpi.ca_implemented_solution || '';
      const existingEV = kpi.ca_evidence || '';
      const caStatus = kpi.ca_status || '';

      kpiCardsHtml += `
        <div class="kpi-card" data-kpi-id="${kpi.kpi_id}" data-low-limit="${lowLimit !== null ? lowLimit : ''}">
       <div class="kpi-header">
       <div class="kpi-title">${kpi.subject}</div>

       <div class="kpi-limits">
        ${lowLimit !== null ? `<span class="limit-badge low">Low: ${lowLimit} ${kpi.unit || ''}</span>` : ''}

        ${(kpi.high_limit && kpi.high_limit !== 'None' && kpi.high_limit !== 'null' && kpi.high_limit !== '' && !isNaN(parseFloat(kpi.high_limit)))
          ? `<span class="limit-badge high">High: ${parseFloat(kpi.high_limit)} ${kpi.unit || ''}</span>`
          : ''
        }
         </div>
         </div>
          ${kpi.indicator_sub_title ? `<div class="kpi-subtitle">${kpi.indicator_sub_title}</div>` : ''}
          <input type="number" step="any" name="value_${kpi.kpi_values_id}"
                 value="${kpi.value || ''}" placeholder="Enter value"
                 class="kpi-input value-input"
                 data-kpi-values-id="${kpi.kpi_values_id}"
                 data-low-limit="${lowLimit !== null ? lowLimit : ''}" required />
      

          <!-- ===== CORRECTIVE ACTION PANEL ===== -->
          <div class="ca-container ${showCA ? 'visible' : ''}"
               id="ca_container_${kpi.kpi_values_id}">

            <!-- Header row -->
            <div class="ca-header">
              ⚠️ Value is below low limit — corrective action required
              ${caStatus ? `<span class="ca-status-badge ca-status-${caStatus.toLowerCase().replace(/ /g, '-')}">${caStatus}</span>` : ''}
            </div>

            <!-- AI Suggestion Box -->
            <div class="ca-ai-box" id="ca-ai-box-${kpi.kpi_values_id}">
              <div class="ca-ai-header">
                <span style="font-size:16px;">🤖</span>
                <span class="ca-ai-title">AI Corrective Action Suggestion</span>
                <button type="button" class="ca-gen-btn"
                  id="ca-gen-btn-${kpi.kpi_values_id}"
                  onclick="formGenerateSuggestion('${kpi.kpi_values_id}','${kpi.kpi_id}','${responsible_id}','${week}')">
                  <span class="ca-gen-icon">✨</span>
                  <span class="ca-gen-text">Generate Suggestion</span>
                </button>
              </div>

              <!-- Cards shown after generation -->
              <div class="ca-suggestion-content" id="ca-sugg-${kpi.kpi_values_id}" style="display:none;">
                <div class="ca-ai-row">
                  <div class="ca-ai-card ca-rc-card"
                       onclick="formApplyField('root_cause_${kpi.kpi_values_id}',this)">
                    <div class="ca-ai-card-label">
                      <span>🔍</span> Root Cause
                      <span class="ca-apply-hint">Click to apply ↓</span>
                    </div>
                    <div class="ca-ai-card-text" id="ca-rc-text-${kpi.kpi_values_id}"></div>
                  </div>
                  <div class="ca-ai-card ca-sol-card"
                       onclick="formApplyField('impl_solution_${kpi.kpi_values_id}',this)">
                    <div class="ca-ai-card-label">
                      <span>⚡</span> Immediate Action
                      <span class="ca-apply-hint">Click to apply ↓</span>
                    </div>
                    <div class="ca-ai-card-text" id="ca-sol-text-${kpi.kpi_values_id}"></div>
                  </div>
                  <div class="ca-ai-card ca-ev-card"
                       onclick="formApplyField('evidence_${kpi.kpi_values_id}',this)">
                    <div class="ca-ai-card-label">
                      <span>📊</span> Evidence
                      <span class="ca-apply-hint">Click to apply ↓</span>
                    </div>
                    <div class="ca-ai-card-text" id="ca-ev-text-${kpi.kpi_values_id}"></div>
                  </div>
                </div>
              </div>

              <div class="ca-sugg-error" id="ca-err-${kpi.kpi_values_id}" style="display:none;">
                ⚠️ Could not generate suggestion. Please fill manually.
              </div>
            </div>

            <!-- Input fields -->
            <div class="ca-field">
              <label class="ca-label" for="root_cause_${kpi.kpi_values_id}">
                🔍 Root Cause Analysis <span class="ca-required">*</span>
              </label>
              <textarea id="root_cause_${kpi.kpi_values_id}"
                        name="root_cause_${kpi.kpi_values_id}"
                        class="ca-textarea root-cause-input"
                        data-kpi-values-id="${kpi.kpi_values_id}"
                        placeholder="Click 'Generate Suggestion' or describe the root cause manually..."
                        ${showCA ? 'required' : ''}>${existingRC}</textarea>
            </div>

            <div class="ca-field">
              <label class="ca-label" for="impl_solution_${kpi.kpi_values_id}">
                🔧 Implemented Solution <span class="ca-required">*</span>
              </label>
              <textarea id="impl_solution_${kpi.kpi_values_id}"
                        name="impl_solution_${kpi.kpi_values_id}"
                        class="ca-textarea impl-solution-input"
                        data-kpi-values-id="${kpi.kpi_values_id}"
                        placeholder="Click 'Generate Suggestion' or describe the solution taken..."
                        ${showCA ? 'required' : ''}>${existingIS}</textarea>
            </div>

            <div class="ca-field">
              <label class="ca-label" for="evidence_${kpi.kpi_values_id}">
                📎 Evidence of Improvement <span class="ca-required">*</span>
              </label>
              <textarea id="evidence_${kpi.kpi_values_id}"
                        name="evidence_${kpi.kpi_values_id}"
                        class="ca-textarea evidence-input"
                        data-kpi-values-id="${kpi.kpi_values_id}"
                        placeholder="Provide evidence showing the improvement..."
                        ${showCA ? 'required' : ''}>${existingEV}</textarea>
            </div>
          </div>
          <!-- ===== END CORRECTIVE ACTION PANEL ===== -->

          <div class="comment-section">
            <div class="comment-label">Manager Comment <span style="font-size:11px;color:#888;">(Optional)</span></div>
            <textarea name="comment_${kpi.kpi_values_id}" class="comment-input"
                      placeholder="Add your comment...">${kpi.latest_comment || ''}</textarea>
          </div>
        </div>`;
    });

    res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>KPI Form - Week ${week}</title>
      <style>
        body{font-family:'Segoe UI',sans-serif;background:#f4f6f9;
          background-image:url('https://images.unsplash.com/photo-1486262715619-67b85e0b08d3?w=1600');
          background-size:cover;background-position:center;background-attachment:fixed;
          padding:20px;margin:0;min-height:100vh;}
        .container{max-width:800px;margin:0 auto;background:rgba(255,255,255,0.95);
          border-radius:8px;box-shadow:0 4px 20px rgba(0,0,0,0.2);overflow:hidden;}
        .header{background:#0078D7;color:white;padding:20px;text-align:center;}
        .form-section{padding:30px;}
        .info-section{background:#f8f9fa;padding:20px;border-radius:6px;margin-bottom:25px;border-left:4px solid #0078D7;}
        .info-row{display:flex;margin-bottom:15px;align-items:center;}
        .info-label{font-weight:600;color:#333;width:120px;font-size:14px;}
        .info-value{flex:1;padding:8px 12px;background:white;border:1px solid #ddd;border-radius:4px;}
        .kpi-card{background:#fff;border:1px solid #e1e5e9;border-radius:6px;padding:20px;margin-bottom:20px;}
        .kpi-header{
         display:flex;
         justify-content:space-between;
         align-items:center;
         margin-bottom:4px;
        }
        .kpi-title{font-weight:600;color:#333;margin-bottom:5px;font-size:15px;}
       .kpi-title{
        font-weight:600;
        color:#333;
        font-size:15px;
         }

       .kpi-limits{
        display:flex;
        gap:8px;
        }

      .limit-badge{
       font-size:11px;
       font-weight:700;
       padding:3px 10px;
       border-radius:999px;
       border:1px solid transparent;
      }

     .limit-badge.low{
      background:#ffebee;
      color:#c62828;
      border-color:#ef9a9a;
     }

    .limit-badge.high{
     background:#e8f5e9;
     color:#1b5e20;
     border-color:#a5d6a7;
        }
        .kpi-subtitle{color:#666;font-size:13px;margin-bottom:10px;}
        .kpi-input{width:100%;padding:10px 12px;border:1px solid #ddd;border-radius:4px;font-size:14px;box-sizing:border-box;}
        .kpi-input:focus{border-color:#0078D7;outline:none;}
        .submit-btn{background:#0078D7;color:white;border:none;padding:12px 30px;border-radius:4px;
          font-size:16px;font-weight:600;cursor:pointer;display:block;width:100%;margin-top:20px;}
        .unit-label{color:#888;font-size:12px;margin-top:5px;}
        .low-limit-warning{color:#dc3545;font-size:12px;margin-top:5px;font-weight:600;}

        /* ===== Corrective Action Panel ===== */
        .ca-container{
          margin-top:16px;
          background:linear-gradient(135deg,#fff5f5,#fff8f0);
          border:2px solid #f28b82;
          border-radius:10px;
          display:none;
          overflow:hidden;
        }
        .ca-container.visible{display:block;}
        .ca-header{
          font-weight:700;color:#c62828;font-size:14px;
          padding:14px 16px 14px;
          border-bottom:1px dashed #f28b82;
          display:flex;align-items:center;gap:10px;flex-wrap:wrap;
          background:rgba(211,47,47,0.05);
        }
        .ca-status-badge{
          font-size:11px;font-weight:600;padding:3px 10px;border-radius:12px;margin-left:auto;
        }
        .ca-status-open{background:#ffebee;color:#c62828;border:1px solid #ef9a9a;}
        .ca-status-waiting-for-validation{background:#fff3e0;color:#e65100;border:1px solid #ffcc02;}

        /* AI box inside CA panel */
        .ca-ai-box{margin:14px 14px 0;border:1.5px solid #c4b5fd;border-radius:10px;
          background:linear-gradient(135deg,#f5f3ff,#ede9fe);overflow:hidden;}
        .ca-ai-header{display:flex;align-items:center;gap:8px;padding:10px 14px;
          background:rgba(109,40,217,0.08);border-bottom:1px solid #ddd6fe;}
        .ca-ai-title{font-size:13px;font-weight:700;color:#5b21b6;flex:1;}
        .ca-gen-btn{display:inline-flex;align-items:center;gap:5px;padding:7px 14px;
          background:linear-gradient(135deg,#7c3aed,#6d28d9);color:white;border:none;
          border-radius:18px;font-size:12px;font-weight:600;cursor:pointer;
          transition:all 0.2s;box-shadow:0 2px 6px rgba(109,40,217,0.35);}
        .ca-gen-btn:hover:not(:disabled){background:linear-gradient(135deg,#6d28d9,#5b21b6);
          transform:translateY(-1px);}
        .ca-gen-btn:disabled{opacity:0.65;cursor:not-allowed;transform:none;}
        .ca-gen-btn.loading .ca-gen-icon{animation:caGenSpin 1s linear infinite;display:inline-block;}
        @keyframes caGenSpin{to{transform:rotate(360deg);}}
        .ca-suggestion-content{padding:12px 14px;}
        .ca-ai-row{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;}
        @media(max-width:600px){.ca-ai-row{grid-template-columns:1fr;}}
        .ca-ai-card{background:white;border-radius:8px;padding:12px;cursor:pointer;
          transition:transform 0.15s,box-shadow 0.15s;border:1.5px solid transparent;}
        .ca-ai-card:hover{transform:translateY(-2px);box-shadow:0 4px 12px rgba(0,0,0,0.1);}
        .ca-ai-card.applied{border-color:#4ade80!important;background:#f0fdf4;}
        .ca-rc-card{border-top:3px solid #ef4444;}
        .ca-sol-card{border-top:3px solid #f59e0b;}
        .ca-ev-card{border-top:3px solid #3b82f6;}
        .ca-ai-card-label{font-size:10px;font-weight:700;text-transform:uppercase;
          letter-spacing:0.5px;margin-bottom:7px;display:flex;align-items:center;gap:5px;}
        .ca-rc-card .ca-ai-card-label{color:#dc2626;}
        .ca-sol-card .ca-ai-card-label{color:#d97706;}
        .ca-ev-card .ca-ai-card-label{color:#2563eb;}
        .ca-apply-hint{margin-left:auto;font-size:9px;font-weight:500;color:#9ca3af;
          text-transform:none;letter-spacing:0;}
        .ca-ai-card-text{font-size:12px;color:#374151;line-height:1.5;}
        .ca-sugg-error{padding:10px 14px;font-size:12px;color:#92400e;background:#fff7ed;}

        .ca-field{margin-bottom:14px;padding:0 14px;}
        .ca-field:first-of-type{margin-top:14px;}
        .ca-field:last-of-type{margin-bottom:14px;}
        .ca-label{display:block;font-weight:600;font-size:13px;color:#555;margin-bottom:6px;}
        .ca-required{color:#dc3545;}
        .ca-textarea{
          width:100%;padding:10px 12px;border:1.5px solid #f28b82;border-radius:6px;
          min-height:80px;resize:vertical;font-family:inherit;font-size:13px;
          background:#fff;box-sizing:border-box;transition:border-color 0.2s;
        }
        .ca-textarea:focus{border-color:#d32f2f;outline:none;box-shadow:0 0 0 3px rgba(211,47,47,0.12);}
        .ca-textarea.error{border-color:#dc3545;background:#fff5f5;}
        .ca-textarea.highlight{animation:caHighlight 1.8s forwards;}
        @keyframes caHighlight{0%{background:#dcfce7;border-color:#16a34a;}100%{background:#fff;border-color:#f28b82;}}

        .comment-section{margin-top:15px;}
        .comment-label{font-weight:600;color:#555;margin-bottom:8px;font-size:13px;}
        .comment-input{width:100%;padding:10px;border:1px solid #ddd;border-radius:4px;
          min-height:70px;resize:vertical;font-family:inherit;box-sizing:border-box;}
        .loading-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.6);
          z-index:9999;flex-direction:column;align-items:center;justify-content:center;gap:20px;}
        .loading-overlay.active{display:flex;}
        .spinner{width:56px;height:56px;border:6px solid rgba(255,255,255,0.3);
          border-top-color:#fff;border-radius:50%;animation:spin 0.9s linear infinite;}
        @keyframes spin{to{transform:rotate(360deg)}}
        .loading-text{color:#fff;font-size:18px;font-weight:600;}
      </style></head><body>
      <div class="loading-overlay" id="loadingOverlay">
        <div class="spinner"></div>
        <div class="loading-text">Submitting KPI Values...</div>
      </div>
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
      <script>
        // ===== FORM AI SUGGESTION FUNCTIONS =====
        function formApplyField(fieldId, card) {
          const text  = card.querySelector('.ca-ai-card-text').textContent.trim();
          const field = document.getElementById(fieldId);
          if (!field || !text) return;
          field.value = text;
          field.classList.remove('highlight');
          void field.offsetWidth;
          field.classList.add('highlight');
          field.scrollIntoView({ behavior:'smooth', block:'center' });
          card.classList.add('applied');
          const hint = card.querySelector('.ca-apply-hint');
          if (hint) hint.textContent = '✓ Applied';
        }

        async function formGenerateSuggestion(kvId, kpiId, responsibleId, week) {
          const btn     = document.getElementById('ca-gen-btn-' + kvId);
          const suggDiv = document.getElementById('ca-sugg-' + kvId);
          const errDiv  = document.getElementById('ca-err-' + kvId);

          suggDiv.style.display = 'none';
          errDiv.style.display  = 'none';

          // Reset applied state
          suggDiv.querySelectorAll('.ca-ai-card').forEach(c => {
            c.classList.remove('applied');
            const hint = c.querySelector('.ca-apply-hint');
            if (hint) hint.textContent = 'Click to apply ↓';
          });

          btn.disabled = true;
          btn.classList.add('loading');
          btn.querySelector('.ca-gen-icon').textContent = '⏳';
          btn.querySelector('.ca-gen-text').textContent = 'Generating...';

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
            document.getElementById('ca-rc-text-'  + kvId).textContent = s.root_cause       || '';
            document.getElementById('ca-sol-text-' + kvId).textContent = s.immediate_action || '';
            document.getElementById('ca-ev-text-'  + kvId).textContent = s.evidence         || '';
            suggDiv.style.display = 'block';
          } catch (err) {
            errDiv.style.display = 'block';
          } finally {
            btn.disabled = false;
            btn.classList.remove('loading');
            btn.querySelector('.ca-gen-icon').textContent = '🔄';
            btn.querySelector('.ca-gen-text').textContent = 'Regenerate';
          }
        }

        // Show/hide corrective action panel based on value vs low limit
        function checkLowLimit(input) {
          const card = input.closest('.kpi-card');
          const llStr = card.dataset.lowLimit;
          let ll = null;
          if (llStr && llStr !== '' && llStr !== 'null' && !isNaN(parseFloat(llStr))) ll = parseFloat(llStr);
          const val = parseFloat(input.value);
          const kvId = input.dataset.kpiValuesId;
          const caPanel = document.getElementById('ca_container_' + kvId);

          if (caPanel && ll !== null && !isNaN(val)) {
            const isBelow = val < ll;
            caPanel.classList.toggle('visible', isBelow);

            // Toggle required on the three textareas
            ['root_cause_','impl_solution_','evidence_'].forEach(prefix => {
              const ta = document.getElementById(prefix + kvId);
              if (ta) ta.required = isBelow;
            });
          }
        }

        document.querySelectorAll('.value-input').forEach(i => {
          i.addEventListener('input', function(){ checkLowLimit(this); });
          checkLowLimit(i);
        });

        document.getElementById('kpiForm').addEventListener('submit', function(e) {
          let hasError = false;

          // Validate numeric value inputs
          this.querySelectorAll('input.value-input[required]').forEach(input => {
            if (!input.value.trim() || isNaN(input.value.trim())) {
              e.preventDefault(); hasError = true;
              input.style.borderColor = '#dc3545';
              const err = input.nextElementSibling;
              if (err && err.classList.contains('field-error')) err.style.display = 'block';
            }
          });

          // Validate corrective action textareas when panel is visible
          document.querySelectorAll('.ca-container.visible').forEach(panel => {
            panel.querySelectorAll('.ca-textarea').forEach(ta => {
              if (!ta.value.trim()) {
                e.preventDefault(); hasError = true;
                ta.classList.add('error');
              } else {
                ta.classList.remove('error');
              }
            });
          });

          if (!hasError) document.getElementById('loadingOverlay').classList.add('active');
        });

        // Clear error state on typing
        document.querySelectorAll('.ca-textarea').forEach(ta => {
          ta.addEventListener('input', function(){ this.classList.remove('error'); });
        });
      </script></body></html>`);
  } catch (err) {
    res.send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});

// ---------- Dashboard ----------
app.get("/dashboard", async (req, res) => {
  try {
    const { responsible_id } = req.query;
    const resResp = await pool.query(
      `SELECT r.*, p.name AS plant_name, d.name AS department_name
       FROM public."Responsible" r JOIN public."Plant" p ON r.plant_id = p.plant_id
       JOIN public."Department" d ON r.department_id = d.department_id
       WHERE r.responsible_id = $1`, [responsible_id]);
    const responsible = resResp.rows[0];
    if (!responsible) throw new Error("Responsible not found");
    const kpiRes = await pool.query(
      `SELECT DISTINCT ON (h.week, h.kpi_id)
              h.hist_id, h.kpi_values_id, h.new_value as value, h.week,
              h.kpi_id, h.updated_at,
              k.subject, k.indicator_sub_title, k.unit, k.target, k.min, k.max,
              k.tolerance_type, k.up_tolerance, k.low_tolerance, k.frequency,
              k.definition, k.calculation_on, k.target_auto_adjustment,
              k.high_limit, k.low_limit
       FROM public.kpi_values_hist26 h JOIN public."Kpi" k ON h.kpi_id = k.kpi_id
       WHERE h.responsible_id = $1
       ORDER BY h.week DESC, h.kpi_id ASC, h.updated_at DESC`, [responsible_id]);
    const weekMap = new Map();
    kpiRes.rows.forEach(kpi => {
      if (!weekMap.has(kpi.week)) weekMap.set(kpi.week, []);
      weekMap.get(kpi.week).push(kpi);
    });
    let html = `<!DOCTYPE html><html><head><meta charset="utf-8"><title>KPI Dashboard</title>
      <style>body{font-family:'Segoe UI',sans-serif;background:#f4f6f9;
        background-image:url('https://images.unsplash.com/photo-1486262715619-67b85e0b08d3?w=1600');
        background-size:cover;background-position:center;background-attachment:fixed;padding:20px;margin:0;}
      .container{max-width:900px;margin:0 auto;}
      .header{background:#0078D7;color:white;padding:20px;text-align:center;border-radius:8px 8px 0 0;}
      .content{background:#fff;padding:30px;border-radius:0 0 8px 8px;box-shadow:0 2px 10px rgba(0,0,0,0.1);}
      .info-section{background:#f8f9fa;padding:20px;border-radius:6px;margin-bottom:25px;border-left:4px solid #0078D7;}
      .info-row{display:flex;margin-bottom:10px;}
      .info-label{width:120px;font-weight:600;color:#333;}
      .info-value{flex:1;background:white;padding:8px 12px;border:1px solid #ddd;border-radius:4px;}
      .week-section{margin-bottom:30px;border:1px solid #e1e5e9;border-radius:8px;padding:20px;background:#fafbfc;}
      .week-title{color:#0078D7;font-size:20px;margin-bottom:15px;font-weight:600;
        border-bottom:2px solid #0078D7;padding-bottom:8px;}
      .kpi-card{background:#fff;border:1px solid #e1e5e9;border-radius:6px;padding:15px;margin-bottom:15px;}
      .kpi-title{font-weight:600;color:#333;margin-bottom:5px;font-size:16px;}
      .kpi-date{color:#999;font-size:11px;margin-top:3px;font-style:italic;}
      </style></head><body>
      <div class="container">
        <div class="header"><h1>KPI Dashboard - ${responsible.name}</h1></div>
        <div class="content">
          <div class="info-section">
            <div class="info-row"><div class="info-label">Responsible</div><div class="info-value">${responsible.name}</div></div>
            <div class="info-row"><div class="info-label">Group</div><div class="info-value">${responsible.plant_name}</div></div>
            <div class="info-row"><div class="info-label">Department</div><div class="info-value">${responsible.department_name}</div></div>
          </div>`;
    if (weekMap.size === 0) {
      html += `<div style="color:#999;font-style:italic;">No KPI data available yet.</div>`;
    } else {
      for (const [week, items] of weekMap) {
        html += `<div class="week-section"><div class="week-title">📅 Week ${week}</div>`;
        items.forEach(kpi => {
          const hasValue = kpi.value !== null && kpi.value !== undefined && kpi.value !== '';
          const dotColor = hasValue ? getDotColor(kpi.value, kpi.low_limit, kpi.high_limit) : '#6c757d';
          const submitted = kpi.updated_at
            ? new Date(kpi.updated_at).toLocaleString('en-GB', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' }) : '';
          html += `
            <div class="kpi-card">
              <div class="kpi-title">${kpi.subject}</div>
              ${kpi.indicator_sub_title ? `<div style="color:#666;font-size:13px;font-style:italic;">${kpi.indicator_sub_title}</div>` : ''}
              <div style="display:flex;justify-content:space-between;align-items:center;margin:12px 0;">
                ${kpi.target ? `<div><span style="font-weight:600;color:#495057;">Target: </span>
                  <span style="color:#28a745;font-weight:700;">${parseFloat(kpi.target).toLocaleString()} ${kpi.unit || ''}</span></div>` : ''}
                <div><span style="font-weight:600;color:#495057;">Actual: </span>
                  <span style="font-size:20px;font-weight:700;color:${dotColor};">
                    ${hasValue ? kpi.value : 'Not filled'} ${kpi.unit || ''}</span></div>
              </div>
              ${kpi.high_limit ? `<div style="font-size:12px;color:#ff9800;">🔺 High Limit: ${kpi.high_limit} ${kpi.unit || ''}</div>` : ''}
              ${kpi.low_limit ? `<div style="font-size:12px;color:#dc3545;">🔻 Low Limit: ${kpi.low_limit} ${kpi.unit || ''}</div>` : ''}
              ${submitted ? `<div class="kpi-date">Last updated: ${submitted}</div>` : ''}
            </div>`;
        });
        html += `</div>`;
      }
    }
    html += `</div></div></body></html>`;
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

  // Dot color helper (assumes you already have getDotColor defined)
  const pointColors = values.map(v => getDotColor(v, cleanLow, cleanHigh));

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
  const trendIcon = (() => {
    if (cleanLow !== null) {
      if (currentValue < cleanLow) return { icon: '↓', color: '#dc2626' };       // red - below limit
      if (currentValue < cleanLow * 1.10) return { icon: '→', color: '#ff9800' }; // orange - near limit
    }
    return { icon: '↑', color: '#28a745' }; // green - safe
  })();

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
              <div style="font-size:32px;font-weight:700;color:${getDotColor(currentValue, cleanLow, cleanHigh)};line-height:36px;">
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
            Week ${ca.week ? String(ca.week).replace('2026-Week', '') : 'N/A'}
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
        LEFT JOIN public.corrective_actions ca
               ON ca.kpi_id = h.kpi_id
              AND ca.responsible_id = h.responsible_id
              AND ca.week = h.week
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
       FROM public."Responsible" r JOIN public."Plant" p ON r.plant_id = p.plant_id
       JOIN public."Department" d ON r.department_id = d.department_id
       WHERE r.responsible_id = $1`, [responsibleId]);
    const responsible = resResp.rows[0];
    if (!responsible) throw new Error(`Responsible ${responsibleId} not found`);

    const chartsData = await generateWeeklyReportData(responsibleId, reportWeek);
    let chartsHtml = '';
    let hasData = false;

    if (chartsData && chartsData.length > 0) {
      hasData = true;
      chartsData.forEach(chart => { chartsHtml += generateVerticalBarChart(chart); });
    } else {
      chartsHtml = `<div style="text-align:center;padding:60px;background:#f8f9fa;border-radius:12px;">
        <div style="font-size:48px;color:#adb5bd;margin-bottom:20px;">📊</div>
        <p style="color:#495057;margin:0;font-size:18px;">No KPI Data Available</p></div>`;
    }

    const emailHtml = `<!DOCTYPE html><html><head><meta charset="utf-8"></head>
    <body style="margin:0;padding:0;font-family:Arial,sans-serif;background:#f4f6f9;">
      <table border="0" cellpadding="0" cellspacing="0" width="100%" style="background:#f4f6f9;">
        <tr><td align="center" style="padding:20px;">
          <table border="0" cellpadding="0" cellspacing="0" width="100%">
            <tr><td style="background:#0078D7;padding:30px;text-align:center;border-radius:8px 8px 0 0;">
              <h1 style="margin:0;color:white;font-size:24px;">📊 KPI Performance Report</h1>
              <p style="margin:10px 0 20px;color:rgba(255,255,255,0.9);">
                ${reportWeek.replace('2026-Week', 'Week ')} | ${responsible.name} | ${responsible.plant_name}</p>
              <table border="0" cellpadding="0" cellspacing="0" align="center"><tr>
                <td style="padding:0 8px;">
                  <a href="https://kpi-codir.azurewebsites.net/kpi-trends?responsible_id=${responsible.responsible_id}"
                     style="display:inline-block;padding:12px 24px;background:#38bdf8;color:white;
                            text-decoration:none;border-radius:8px;font-weight:600;font-size:14px;">
                    📈 View KPI Graphics</a></td>
                <td style="padding:0 8px;">
                  <a href="https://kpi-codir.azurewebsites.net/dashboard?responsible_id=${responsible.responsible_id}"
                     style="display:inline-block;padding:12px 24px;background:#38bdf8;color:white;
                            text-decoration:none;border-radius:8px;font-weight:600;font-size:14px;">
                    📊 View Dashboard</a></td>
              </tr></table>
            </td></tr>

            <!-- ── Recommendations note ───────────────────────────── -->
            <tr><td style="padding:20px 30px 0;">
              <div style="background:#fff8e1;border:1px solid #ffe082;border-radius:8px;padding:14px 18px;">
                <span style="font-size:14px;color:#5f4200;">
                  📎 <strong>AI Recommendations PDF is attached</strong> — open it for root-cause analysis,
                  action plans and improvement roadmaps for each KPI.
                </span>
              </div>
            </td></tr>
            <!-- ───────────────────────────────────────────────────── -->

            <tr><td style="padding:30px;">${chartsHtml}</td></tr>
            <tr><td style="padding:20px;background:#f8f9fa;border-top:1px solid #e9ecef;
                            text-align:center;font-size:12px;color:#666;">
              AVOCarbon KPI System • Generated ${new Date().toLocaleDateString('en-GB')}
            </td></tr>
          </table>
        </td></tr>
      </table>
    </body></html>`;

    // ── Generate recommendations PDF to attach ────────────────────────────────
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
      // Don't block the main email if PDF generation fails
      console.error(`⚠️ Could not generate recommendations PDF for ${responsible.name}:`, pdfErr.message);
    }

    // ── Send the single combined email ────────────────────────────────────────
    const transporter = createTransporter();
    const mailOptions = {
      from: '"AVOCarbon KPI System" <administration.STS@avocarbon.com>',
      to: responsible.email,
      subject: `📊 KPI Performance Trends - ${reportWeek} | ${responsible.name}`,
      html: emailHtml,
      attachments: pdfAttachment ? [pdfAttachment] : [],
    };

    await transporter.sendMail(mailOptions);
    console.log(`✅ Weekly report + recommendations PDF sent to ${responsible.email}`);

  } catch (error) {
    console.error(`❌ Failed to send weekly report to ${responsibleId}:`, error.message);
    throw error;
  }
};
// ---------- Cron: weekly KPI submission email ----------
let cronRunning = false;
cron.schedule("02 16 * * *", async () => {
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
    const targetWeek = currentWeek - 1;
    const targetYear = targetWeek < 1 ? now.getFullYear() - 1 : now.getFullYear();
    const forcedWeek = `${targetYear}-Week${targetWeek < 1 ? 52 : targetWeek}`;
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
cron.schedule("34 10 * * *", async () => {
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

  // Arrow DIRECTION = value movement (up/down/flat)
  // Arrow DIRECTION = based on color status (green=up, red=down, orange=flat)
  const trendColor = (() => {
    if (low_limit !== null) {
      if (currentValue < low_limit) return '#dc2626';
      if (currentValue < low_limit * 1.10) return '#f97316';
    }
    if (high_limit !== null && currentValue > high_limit) return '#f97316';
    return '#22c55e';
  })();

  const trendArrow = trendColor === '#22c55e' ? '↗'
    : trendColor === '#dc2626' ? '↘'
      : '→';



  const pointColors = values.map(v => {
    if (low_limit !== null) {
      if (v < low_limit) return '#dc2626';
      if (v < low_limit * 1.10) return '#f97316';
    }
    return '#22c55e';
  });

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
                      font-weight:700;font-size:12px;display:inline-block;">${trendArrow}</div>
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
                  <td width="10" height="10" style="background:#f97316;border-radius:50%;font-size:0;">&nbsp;</td>
                  <td style="font-size:10px;color:#666;">Near low</td>
                </tr></table></td>
                <td><table border="0" cellpadding="0" cellspacing="3"><tr>
                  <td width="10" height="10" style="background:#dc2626;border-radius:50%;font-size:0;">&nbsp;</td>
                  <td style="font-size:10px;color:#666;">Below low</td>
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
    kpi.colors.push(getDotColor(value, kpi.low_limit, kpi.high_limit));
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
    if (kpi.low_limit !== null) {
      const latest = vals[vals.length - 1];
      kpi.achievementVsLimit = latest >= kpi.low_limit;
      kpi.achievementColor = getDotColor(latest, kpi.low_limit, kpi.high_limit);
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
          <span style="font-size:12px;color:#6b7280;">Above low limit</span></div>
        <div style="display:flex;align-items:center;gap:6px;">
          <div style="width:12px;height:12px;border-radius:50%;background:#f97316;"></div>
          <span style="font-size:12px;color:#6b7280;">Near low limit (≤10%)</span></div>
        <div style="display:flex;align-items:center;gap:6px;">
          <div style="width:12px;height:12px;border-radius:50%;background:#ef4444;"></div>
          <span style="font-size:12px;color:#6b7280;">Below low limit</span></div>
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

  const pointColors = JSON.stringify(kpi.values.map(v => {
    const val = parseFloat(v);
    if (isNaN(val)) return '#6b7280';
    if (lowLimit !== null) {
      if (val < lowLimit) return '#ef4444';
      if (val < lowLimit * 1.10) return '#f97316';
    }
    return '#22c55e';
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
       LEFT JOIN public.corrective_actions ca
         ON ca.kpi_id = lkv.kpi_id
        AND ca.responsible_id = lkv.responsible_id
        AND ca.week = lkv.week
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
cron.schedule("29 10 * * *", async () => {
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
