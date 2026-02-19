require("dotenv").config();
const express = require("express");
const { Pool } = require("pg");
const bodyParser = require("body-parser");
const cors = require("cors");
const nodemailer = require("nodemailer");
const cron = require("node-cron");

const app = express();
const port = process.env.PORT || 5000;

app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// ---------- Postgres ----------
const pool = new Pool({
  user: "administrationSTS",
  host: "avo-adb-002.postgres.database.azure.com",
  database: "kpi_codir",
  password: "St$@0987",
  port: 5432,
  ssl: { rejectUnauthorized: false },
});

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

// ---------- Fix getResponsibleWithKPIs function ----------
const getResponsibleWithKPIs = async (responsibleId, week) => {
  const resResp = await pool.query(
    `
    SELECT r.responsible_id, r.name, r.email, r.plant_id, r.department_id,
           p.name AS plant_name, d.name AS department_name
    FROM public."Responsible" r
    JOIN public."Plant" p ON r.plant_id = p.plant_id
    JOIN public."Department" d ON r.department_id = d.department_id
    WHERE r.responsible_id = $1
    `,
    [responsibleId]
  );

  const responsible = resResp.rows[0];
  if (!responsible) throw new Error("Responsible not found");

  const kpiRes = await pool.query(
    `
    SELECT kv.kpi_values_id, kv.value, kv.week, k.kpi_id, 
           k.subject, k.indicator_sub_title, k.unit,
           k.target, k.min, k.max, k.tolerance_type,
           k.up_tolerance, k.low_tolerance, k.frequency,
           k.definition, k.calculation_on, k.target_auto_adjustment,
           -- Get latest update time from history or use current time
           COALESCE(
             (SELECT MAX(updated_at) 
              FROM public.kpi_values_hist26 
              WHERE kpi_values_id = kv.kpi_values_id),
             NOW()
           ) as last_updated
    FROM public.kpi_values kv
    JOIN "Kpi" k ON kv.kpi_id = k.kpi_id
    WHERE kv.responsible_id = $1 AND kv.week = $2
    ORDER BY k.kpi_id ASC
    `,
    [responsibleId, week]
  );

  return { responsible, kpis: kpiRes.rows };
};

// ---------- Generate Email HTML with Button ----------
const generateEmailHtml = ({ responsible, week }) => {
  return `
  <!DOCTYPE html>
  <html>
  <head><meta charset="utf-8"><title>KPI Form</title></head>
  <body style="font-family:'Segoe UI',sans-serif;background:#f4f4f4;padding:20px;">
    <div style="max-width:600px;margin:0 auto;background:#fff;padding:25px;
                border-radius:10px;box-shadow:0 4px 15px rgba(0,0,0,0.1);text-align:center;">
      
      <img src="https://media.licdn.com/dms/image/v2/D4E0BAQGYVmAPO2RZqQ/company-logo_200_200/company-logo_200_200/0/1689240189455/avocarbon_group_logo?e=2147483647&v=beta&t=nZNCXd3ypoMFQnQMxfAZrljyNBbp4E5HM11Y1yl9_L0" 
           alt="AVOCarbon Logo" style="width:80px;height:80px;object-fit:contain;margin-bottom:20px;">
      
      <h2 style="color:#0078D7;font-size:22px;margin-bottom:20px;">KPI Submission - ${week}</h2>
    
      <h3 style="color:#0078D7;font-size:16px;margin-bottom:20px;">
           ${responsible.plant_name}
      </h3>
          
      <a href="http://localhost:5000/form?responsible_id=${responsible.responsible_id}&week=${week}"
         style="display:inline-block;padding:12px 20px;background:#0078D7;color:white;
                border-radius:8px;text-decoration:none;font-weight:bold;font-size:16px;">
        Fill KPI Form
      </a>

      <p style="margin-top:20px;font-size:12px;color:#888;">
        Click the button above to fill your KPIs for week ${week}.
      </p>
    </div>
  </body>
  </html>
  `;
};



// Helper function to parse target values safely
const parsetargetValue = (targetValue) => {
  if (targetValue === null || targetValue === undefined || targetValue === '') {
    return null;
  }

  // Try to parse as number
  const parsed = parseFloat(targetValue);
  return isNaN(parsed) ? null : parsed;
};


// Send consolidated target update email
const sendConsolidatedTargetUpdateEmail = async (responsibleId, week, targetUpdates) => {
  try {
    // Get responsible info
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
    if (!responsible || !responsible.email) {
      console.log(`No email found for responsible ${responsibleId}`);
      return null;
    }

    if (targetUpdates.length === 0) {
      console.log(`No target updates to send for responsible ${responsibleId}, week ${week}`);
      return null;
    }

    // Get KPI details for each update
    const enhancedUpdates = [];
    for (const update of targetUpdates) {
      const kpiRes = await pool.query(
        `SELECT indicator_title, indicator_sub_title, unit
         FROM public."Kpi"
         WHERE kpi_id = $1`,
        [update.kpiId]
      );

      if (kpiRes.rows[0]) {
        const kpi = kpiRes.rows[0];
        const improvement = ((update.newTarget - update.oldTarget) / update.oldTarget * 100).toFixed(1);

        enhancedUpdates.push({
          kpiId: update.kpiId,
          indicator_title: kpi.indicator_title,
          indicator_sub_title: kpi.indicator_sub_title,
          unit: kpi.unit || '',
          oldTarget: update.oldTarget,
          newTarget: update.newTarget,
          improvement: improvement
        });
      }
    }

    const html = generateConsolidatedTargetUpdateEmailHtml({
      responsible,
      week,
      targetUpdates: enhancedUpdates
    });

    const transporter = createTransporter();
    const info = await transporter.sendMail({
      from: '"AVOCarbon KPI System" <administration.STS@avocarbon.com>',
      to: responsible.email,
      subject: `🎯 ${targetUpdates.length} KPI Target${targetUpdates.length > 1 ? 's' : ''} Updated - Week ${week}`,
      html,
    });

    console.log(`✅ Consolidated target update email sent to ${responsible.email} (${targetUpdates.length} KPIs)`);
    return info;
  } catch (err) {
    console.error(`❌ Failed to send consolidated target update email:`, err.message);
    return null;
  }
};



const checkAndTriggerCorrectiveActions = async (
  responsibleId,
  kpiId,
  week,
  newValue,
  histId
) => {
  try {
    // 1️⃣ Get current target value
    const kpiRes = await pool.query(
      `SELECT target FROM public."Kpi" WHERE kpi_id = $1`,
      [kpiId]
    );

    if (!kpiRes.rows.length) return { targetUpdated: false };

    const currentTarget = parseFloat(kpiRes.rows[0].target);
    const numValue = parseFloat(newValue);

    if (isNaN(numValue) || isNaN(currentTarget)) return { targetUpdated: false };

    // 2️⃣ Always update history with current target
    await pool.query(
      `UPDATE public.kpi_values_hist26
       SET target = $1
       WHERE responsible_id = $2
         AND kpi_id = $3
         AND week = $4
         AND (target IS NULL OR target < $1)`,
      [numValue, responsibleId, kpiId, week]
    );

    console.log(`📝 History updated with target: ${currentTarget} for hist_id: ${histId}`);

    // 3️⃣ If value exceeds target → update KPI target and RETURN update info
    if (numValue > currentTarget) {
      const oldTarget = currentTarget;

      // ✅ Update KPI target
      await pool.query(
        `UPDATE public."Kpi"
         SET target = $1
         WHERE kpi_id = $2`,
        [numValue, kpiId]
      );

      console.log(`🎯 Updated target for KPI ${kpiId}: ${oldTarget} → ${numValue}`);

      // ✅ Return target update info (NO EMAIL SENT HERE)
      return {
        targetUpdated: true,
        updateInfo: {
          kpiId: kpiId,
          oldTarget: oldTarget,
          newTarget: numValue
        }
      };
    }

    // 4️⃣ If value is BELOW target → create corrective action
    if (numValue < currentTarget) {
      const existingCA = await pool.query(
        `SELECT corrective_action_id
         FROM public.corrective_actions
         WHERE responsible_id = $1
           AND kpi_id = $2
           AND week = $3
           AND status = 'Open'`,
        [responsibleId, kpiId, week]
      );

      if (existingCA.rows.length === 0) {
        await pool.query(
          `INSERT INTO public.corrective_actions
           (responsible_id, kpi_id, week, status)
           VALUES ($1, $2, $3, 'Open')`,
          [responsibleId, kpiId, week]
        );

        console.log(`🔴 Corrective action created for KPI ${kpiId}, Week ${week}`);
      }
    }

    return { targetUpdated: false };

  } catch (error) {
    console.error(`Error checking corrective actions:`, error.message);
    return { targetUpdated: false, error: error.message };
  }
};
// ========== CORRECTIVE ACTION SYSTEM - END ==========



// ========== VIEW CORRECTIVE ACTIONS LIST ==========
app.get("/corrective-actions-list", async (req, res) => {
  try {
    const { responsible_id } = req.query;

    const actionsRes = await pool.query(
      `SELECT ca.*, k.indicator_title, k.indicator_sub_title, k.unit
      FROM public.corrective_actions ca
      JOIN public."Kpi" k ON ca.kpi_id = k.kpi_id
      WHERE ca.responsible_id = $1
      ORDER BY ca.created_date DESC`,
      [responsible_id]
    );

    const actions = actionsRes.rows;

    res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <title>Corrective Actions History</title>
        <style>
          body { font-family: 'Segoe UI', sans-serif; background: #f4f6f9; padding: 20px; margin: 0; }
          .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
          h1 { color: #0078D7; margin-bottom: 25px; }
          table { width: 100%; border-collapse: collapse; margin-top: 20px; }
          th, td { padding: 12px; border: 1px solid #ddd; text-align: left; }
          th { background: #0078D7; color: white; font-weight: 600; }
          tr:nth-child(even) { background: #f8f9fa; }
          .status-badge { display: inline-block; padding: 4px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; }
          .status-open { background: #ffebee; color: #c62828; }
          .status-completed { background: #e8f5e9; color: #2e7d32; }
          .action-link { color: #0078D7; text-decoration: none; font-weight: 600; }
          .action-link:hover { text-decoration: underline; }
        </style>
      </head>
      <body>
        <div class="container">
          <h1>📋 Corrective Actions History</h1>
          <table>
            <thead>
              <tr>
                <th>Week</th>
                <th>KPI</th>
                <th>Status</th>
                <th>Created</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              ${actions.length === 0 ?
        '<tr><td colspan="5" style="text-align:center;padding:40px;color:#999;">No corrective actions found</td></tr>'
        :
        actions.map(action => `
                  <tr>
                    <td>${action.week}</td>
                    <td>
                      <strong>${action.indicator_title}</strong>
                      ${action.indicator_sub_title ? `<br><small>${action.indicator_sub_title}</small>` : ''}
                    </td>
                    <td>
                      <span class="status-badge status-${action.status.toLowerCase()}">
                        ${action.status}
                      </span>
                    </td>
                    <td>${new Date(action.created_date).toLocaleDateString()}</td>
                    <td>
                      <a href="/corrective-action-form?responsible_id=${responsible_id}&kpi_id=${action.kpi_id}&week=${action.week}" class="action-link">
                        ${action.status === 'Open' ? 'Complete' : 'View'}
                      </a>
                    </td>
                  </tr>`
        ).join('')}
            </tbody>
          </table>
        </div>
      </body>
      </html>
    `);
  } catch (err) {
    console.error("Error loading corrective actions list:", err);
    res.status(500).send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});


// ========== CORRECTIVE ACTION FORM PAGE ==========
app.get("/corrective-action-form", async (req, res) => {
  try {
    const { responsible_id, kpi_id, week } = req.query;

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

    const kpiResp = await pool.query(
      `SELECT k.kpi_id, k.indicator_title, k.indicator_sub_title, k.unit, k.target,
              kv.value
       FROM public."Kpi" k
       LEFT JOIN public.kpi_values kv ON k.kpi_id = kv.kpi_id 
       WHERE k.kpi_id = $1 AND kv.responsible_id = $2 AND kv.week = $3`,
      [kpi_id, responsible_id, week]
    );

    const kpi = kpiResp.rows[0];
    if (!kpi) return res.status(404).send("KPI not found");

    const existingCA = await pool.query(
      `SELECT * FROM public.corrective_actions
       WHERE responsible_id = $1 AND kpi_id = $2 AND week = $3
       ORDER BY created_date DESC LIMIT 1`,
      [responsible_id, kpi_id, week]
    );

    const existingData = existingCA.rows[0] || {};

    res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <title>Corrective Action Form</title>
        <style>
          body { font-family: 'Segoe UI', sans-serif; background: #f4f6f9; padding: 20px; margin: 0; }
          .container { max-width: 800px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); overflow: hidden; }
          .header { background: linear-gradient(135deg, #d32f2f 0%, #f44336 100%); color: white; padding: 25px; text-align: center; }
          .header h1 { margin: 0; font-size: 24px; font-weight: 600; }
          .alert-badge { background: rgba(255,255,255,0.2); display: inline-block; padding: 8px 16px; border-radius: 20px; margin-top: 10px; font-size: 13px; }
          .form-section { padding: 30px; }
          .info-box { background: #fff3e0; border-left: 4px solid #ff9800; padding: 20px; margin-bottom: 25px; border-radius: 4px; }
          .info-row { display: flex; margin-bottom: 12px; font-size: 14px; }
          .info-label { font-weight: 600; width: 140px; color: #333; }
          .info-value { flex: 1; color: #666; }
          .performance-box { display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin: 20px 0; }
          .perf-item { text-align: center; padding: 15px; background: #f8f9fa; border-radius: 6px; }
          .perf-label { font-size: 11px; color: #666; margin-bottom: 8px; text-transform: uppercase; }
          .perf-value { font-size: 24px; font-weight: 700; }
          .perf-value.current { color: #d32f2f; }
          .perf-value.target{ color: #4caf50; }
          .perf-value.gap { color: #ff9800; }
          .form-group { margin-bottom: 25px; }
          label { display: block; font-weight: 600; color: #333; margin-bottom: 8px; font-size: 14px; }
          label .required { color: #d32f2f; margin-left: 4px; }
          textarea { width: 100%; padding: 12px; border: 1px solid #ddd; border-radius: 4px; font-size: 14px; font-family: 'Segoe UI', sans-serif; box-sizing: border-box; min-height: 100px; resize: vertical; }
          textarea:focus { border-color: #d32f2f; outline: none; box-shadow: 0 0 0 2px rgba(211,47,47,0.1); }
          .help-text { font-size: 12px; color: #666; margin-top: 5px; font-style: italic; }
          .submit-btn { background: #d32f2f; color: white; border: none; padding: 14px 30px; border-radius: 6px; font-size: 16px; font-weight: 600; cursor: pointer; width: 100%; }
          .submit-btn:hover { background: #b71c1c; }
          .status-badge { display: inline-block; padding: 6px 12px; border-radius: 12px; font-size: 12px; font-weight: 600; }
          .status-open { background: #ffebee; color: #c62828; }
          .status-completed { background: #e8f5e9; color: #2e7d32; }
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <div style="font-size: 40px;margin-bottom:10px;">⚠️</div>
            <h1>Corrective Action Form</h1>
            <div class="alert-badge">Week ${week} - Performance Below target_value</div>
          </div>

          <div class="form-section">
            <div class="info-box">
              <div class="info-row">
                <div class="info-label">Responsible:</div>
                <div class="info-value">${responsible.name}</div>
              </div>
              <div class="info-row">
                <div class="info-label">Plant:</div>
                <div class="info-value">${responsible.plant_name}</div>
              </div>
              <div class="info-row">
                <div class="info-label">Department:</div>
                <div class="info-value">${responsible.department_name}</div>
              </div>
              <div class="info-row">
                <div class="info-label">KPI:</div>
                <div class="info-value">${kpi.indicator_title}</div>
              </div>
            </div>

            <div class="performance-box">
              <div class="perf-item">
                <div class="perf-label">Current Value</div>
                <div class="perf-value current">${kpi.value || '0'}${kpi.unit || ''}</div>
              </div>
              <div class="perf-item">
                <div class="perf-label">target_value</div>
                <div class="perf-value target_value">${kpi.target|| 'N/A'}${kpi.unit || ''}</div>
              </div>
              <div class="perf-item">
                <div class="perf-label">Gap</div>
                <div class="perf-value gap">
                  ${kpi.target? (parseFloat(kpi.target_value) - parseFloat(kpi.value || 0)).toFixed(2) : 'N/A'}${kpi.unit || ''}
                </div>
              </div>
            </div>

            ${existingData.corrective_action_id ? `
            <div style="background:#e3f2fd;padding:15px;border-radius:6px;margin-bottom:20px;">
              <strong>Status:</strong> 
              <span class="status-badge status-${existingData.status.toLowerCase()}">${existingData.status}</span>
              <div style="font-size:12px;color:#666;margin-top:8px;">
                Last updated: ${new Date(existingData.updated_date).toLocaleString()}
              </div>
            </div>
            ` : ''}

            <form action="/submit-corrective-action" method="POST">
              <input type="hidden" name="responsible_id" value="${responsible_id}">
              <input type="hidden" name="kpi_id" value="${kpi_id}">
              <input type="hidden" name="week" value="${week}">
              ${existingData.corrective_action_id ?
        `<input type="hidden" name="corrective_action_id" value="${existingData.corrective_action_id}">`
        : ''}

              <div class="form-group">
                <label>Root Cause Analysis<span class="required">*</span></label>
                <textarea name="root_cause" required placeholder="Describe the root cause of the performance gap...">${existingData.root_cause || ''}</textarea>
                <div class="help-text">Use the 5 Whys technique or fishbone diagram to identify the root cause</div>
              </div>

              <div class="form-group">
                <label>Implemented Solution<span class="required">*</span></label>
                <textarea name="implemented_solution" required placeholder="Describe the corrective actions taken...">${existingData.implemented_solution || ''}</textarea>
                <div class="help-text">Detail the specific actions, responsibilities, and timeline</div>
              </div>

              <div class="form-group">
                <label>Evidence of Improvement<span class="required">*</span></label>
                <textarea name="evidence" required placeholder="Provide evidence that the solution is effective...">${existingData.evidence || ''}</textarea>
                <div class="help-text">Include data, observations, or metrics showing improvement</div>
              </div>

              <button type="submit" class="submit-btn">
                ${existingData.corrective_action_id ? '✓ Update' : '📝 Submit'} Corrective Action
              </button>
            </form>
          </div>
        </div>
      </body>
      </html>
    `);
  } catch (err) {
    console.error("Error loading corrective action form:", err);
    res.status(500).send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});

// ========== BULK CORRECTIVE ACTIONS FORM ==========
app.get("/corrective-actions-bulk", async (req, res) => {
  try {
    const { responsible_id, week } = req.query;

    // Get responsible info
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

    // Get all open corrective actions
    const actionsRes = await pool.query(
      `SELECT ca.*, k.indicator_title, k.indicator_sub_title, k.unit, k.target,
              kv.value
       FROM public.corrective_actions ca
       JOIN public."Kpi" k ON ca.kpi_id = k.kpi_id
       LEFT JOIN public.kpi_values kv ON ca.kpi_id = kv.kpi_id 
         AND kv.responsible_id = ca.responsible_id 
         AND kv.week = ca.week
       WHERE ca.responsible_id = $1 
         AND ca.week = $2 
         AND ca.status = 'Open'
       ORDER BY k.indicator_title`,
      [responsible_id, week]
    );

    const actions = actionsRes.rows;
    if (actions.length === 0) {
      return res.send(`
        <div style="text-align:center;padding:60px;font-family:'Segoe UI',sans-serif;">
          <h2 style="color:#4caf50;">✅ No Open Corrective Actions</h2>
          <p>All corrective actions for week ${week} have been completed.</p>
          <a href="/dashboard?responsible_id=${responsible_id}" style="display:inline-block;padding:12px 25px;background:#0078D7;color:white;text-decoration:none;border-radius:6px;font-weight:bold;">Go to Dashboard</a>
        </div>
      `);
    }

    res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <title>Corrective Actions - Week ${week}</title>
        <style>
        body {
          font-family: 'Segoe UI', sans-serif;
          margin: 0;
          padding: 20px;
          min-height: 100vh;
          background-image: url("https://images.unsplash.com/photo-1486262715619-67b85e0b08d3?w=1600");
          background-size: cover;
          background-position: center;
          background-repeat: no-repeat;
          background-attachment: fixed;
         }

        .container {
           max-width: 1000px;
           margin: 0 auto;
           background: rgba(255, 255, 255, 0.95);
           backdrop-filter: blur(5px);
           border-radius: 8px;
           box-shadow: 0 2px 20px rgba(0,0,0,0.3);
           overflow: hidden;
           }

          .header { background: linear-gradient(135deg, #d32f2f 0%, #f44336 100%); color: white; padding: 30px; text-align: center; }
          .header h1 { margin: 0; font-size: 26px; font-weight: 600; }
          .badge { background: rgba(255,255,255,0.2); display: inline-block; padding: 8px 16px; border-radius: 20px; margin-top: 10px; font-size: 14px; }
          .form-section { padding: 30px; }
          .info-box { background: #fff3e0; border-left: 4px solid #ff9800; padding: 20px; margin-bottom: 25px; border-radius: 4px; }
          .kpi-section { margin-bottom: 30px; border: 2px solid #e0e0e0; border-radius: 8px; padding: 20px; background: #fafafa; }
          .kpi-header { display: flex; justify-content: space-between; align-items: start; margin-bottom: 15px; padding-bottom: 15px; border-bottom: 2px solid #e0e0e0; }
          .kpi-title { font-size: 16px; font-weight: 700; color: #333; }
          .kpi-subtitle { font-size: 13px; color: #666; margin-top: 5px; }
          .perf-stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 20px; }
          .stat-box { text-align: center; padding: 12px; background: white; border-radius: 6px; border: 1px solid #e0e0e0; }
          .stat-label { font-size: 10px; color: #666; text-transform: uppercase; margin-bottom: 5px; }
          .stat-value { font-size: 20px; font-weight: 700; }
          .stat-value.current { color: #d32f2f; }
          .stat-value.target { color: #4caf50; }
          .stat-value.gap { color: #ff9800; }
          .form-group { margin-bottom: 20px; }
          label { display: block; font-weight: 600; color: #333; margin-bottom: 8px; font-size: 13px; }
          label .required { color: #d32f2f; }
          textarea { width: 100%; padding: 12px; border: 1px solid #ddd; border-radius: 4px; font-size: 13px; font-family: 'Segoe UI', sans-serif; box-sizing: border-box; min-height: 80px; resize: vertical; }
          textarea:focus { border-color: #d32f2f; outline: none; box-shadow: 0 0 0 2px rgba(211,47,47,0.1); }
          .help-text { font-size: 11px; color: #666; margin-top: 5px; font-style: italic; }
          .submit-btn { background: #d32f2f; color: white; border: none; padding: 16px 40px; border-radius: 6px; font-size: 17px; font-weight: 700; cursor: pointer; width: 100%; margin-top: 20px; }
          .submit-btn:hover { background: #b71c1c; }
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <div style="font-size: 48px; margin-bottom: 15px;">⚠️</div>
            <h1>Corrective Actions Required</h1>
            <div class="badge">${actions.length} KPI${actions.length > 1 ? 's' : ''} Below Target - Week ${week}</div>
          </div>

          <div class="form-section">
            <div class="info-box">
              <strong>${responsible.name}</strong> • ${responsible.plant_name} • ${responsible.department_name}
            </div>

            <form action="/submit-bulk-corrective-actions" method="POST">
              <input type="hidden" name="responsible_id" value="${responsible_id}">
              <input type="hidden" name="week" value="${week}">

              ${actions.map((action, index) => `
                <div class="kpi-section">
                  <input type="hidden" name="corrective_action_ids[]" value="${action.corrective_action_id}">
                  
                  <div class="kpi-header">
                    <div>
                      <div style="display:inline-block;width:30px;height:30px;background:#d32f2f;color:white;border-radius:50%;text-align:center;line-height:30px;font-weight:700;margin-right:10px;">${index + 1}</div>
                      <div style="display:inline-block;vertical-align:top;">
                        <div class="kpi-title">${action.indicator_title}</div>
                        ${action.indicator_sub_title ? `<div class="kpi-subtitle">${action.indicator_sub_title}</div>` : ''}
                      </div>
                    </div>
                  </div>

                  <div class="perf-stats">
                    <div class="stat-box">
                      <div class="stat-label">Current</div>
                      <div class="stat-value current">${action.value || '0'} ${action.unit || ''}</div>
                    </div>
                    <div class="stat-box">
                      <div class="stat-label">Target</div>
                      <div class="stat-value target">${action.target || 'N/A'} ${action.unit || ''}</div>
                    </div>
                    <div class="stat-box">
                      <div class="stat-label">Gap</div>
                      <div class="stat-value gap">
                        ${action.target ? (parseFloat(action.target) - parseFloat(action.value || 0)).toFixed(2) : 'N/A'} ${action.unit || ''}
                      </div>
                    </div>
                  </div>

                  <div class="form-group">
                    <label>Root Cause<span class="required">*</span></label>
                    <textarea name="root_cause_${action.corrective_action_id}" required placeholder="Why did this KPI fall below target?">${action.root_cause || ''}</textarea>
                  </div>

                  <div class="form-group">
                    <label>Implemented Solution<span class="required">*</span></label>
                    <textarea name="solution_${action.corrective_action_id}" required placeholder="What actions have been taken?">${action.implemented_solution || ''}</textarea>
                  </div>

                  <div class="form-group">
                    <label>Evidence<span class="required">*</span></label>
                    <textarea name="evidence_${action.corrective_action_id}" required placeholder="What evidence shows improvement?">${action.evidence || ''}</textarea>
                    <div class="help-text">Provide data, metrics, or observations demonstrating effectiveness</div>
                  </div>
                </div>
              `).join('')}

              <button type="submit" class="submit-btn">
                ✓ Submit All Corrective Actions (${actions.length})
              </button>
            </form>
          </div>
        </div>
      </body>
      </html>
    `);
  } catch (err) {
    console.error("Error loading bulk corrective actions:", err);
    res.status(500).send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});

// ========== SUBMIT BULK CORRECTIVE ACTIONS ==========
app.post("/submit-bulk-corrective-actions", async (req, res) => {
  try {
    const { responsible_id, week, corrective_action_ids, ...formData } = req.body;

    const ids = Array.isArray(corrective_action_ids)
      ? corrective_action_ids
      : [corrective_action_ids];

    let completedCount = 0;

    for (const caId of ids) {
      const rootCause = formData[`root_cause_${caId}`];
      const solution = formData[`solution_${caId}`];
      const evidence = formData[`evidence_${caId}`];

      if (rootCause && solution && evidence) {
        await pool.query(
          `UPDATE public.corrective_actions
           SET root_cause = $1, 
               implemented_solution = $2, 
               evidence = $3,
               status = 'Completed',
               updated_date = NOW()
           WHERE corrective_action_id = $4`,
          [rootCause, solution, evidence, caId]
        );
        completedCount++;
      }
    }

    res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <title>Corrective Actions Submitted</title>
        <style>
          body { font-family: 'Segoe UI', sans-serif; background: #f4f6f9; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
          .success-container { background: white; padding: 50px; border-radius: 10px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); text-align: center; max-width: 600px; }
          h1 { color: #4caf50; font-size: 32px; margin-bottom: 20px; }
          .count { font-size: 48px; font-weight: 700; color: #4caf50; margin: 20px 0; }
          p { font-size: 16px; color: #333; margin-bottom: 30px; line-height: 1.6; }
          a { display: inline-block; padding: 14px 30px; background: #0078D7; color: white; text-decoration: none; border-radius: 6px; font-weight: bold; margin: 5px; }
          a:hover { background: #005ea6; }
        </style>
      </head>
      <body>
        <div class="success-container">
          <h1>✅ All Corrective Actions Submitted!</h1>
          <div class="count">${completedCount}</div>
          <p>
            You have successfully completed all corrective actions for week ${week}.<br>
            The quality team will review your submissions.
          </p>

          <a href="/corrective-actions-list?responsible_id=${responsible_id}">View Corrective Actions</a>
        </div>
      </body>
      </html>
    `);
  } catch (err) {
    console.error("Error submitting bulk corrective actions:", err);
    res.status(500).send(`<h2 style="color:red;">Error: ${err.message}</h2>`);
  }
});
// Generate consolidated corrective action email with ALL KPIs below target
const generateConsolidatedCorrectiveActionEmail = ({ responsible, week, kpisWithActions }) => {
  return `
  <!DOCTYPE html>
  <html>
  <head><meta charset="utf-8"><title>Corrective Actions Required</title></head>
  <body style="font-family:'Segoe UI',sans-serif;background:#f4f4f4;padding:20px;">
    <div style="max-width:700px;margin:0 auto;background:#fff;padding:25px;border-radius:10px;box-shadow:0 4px 15px rgba(0,0,0,0.1);">
      
      <!-- Header -->
      <div style="text-align:center;margin-bottom:30px;">
        <div style="width:90px;height:90px;margin:0 auto 15px;background:#ff9800;border-radius:50%;display:flex;align-items:center;justify-content:center;">
          <span style="font-size:45px;">⚠️</span>
        </div>
        <h2 style="color:#d32f2f;font-size:24px;margin:0;">Corrective Actions Required</h2>
        <p style="color:#666;font-size:14px;margin:10px 0 0 0;">Week ${week} - Multiple KPIs Below Target</p>
      </div>
      
      <!-- Responsible Info -->
      <div style="background:#f8f9fa;padding:20px;border-radius:6px;margin-bottom:25px;border-left:4px solid #d32f2f;">
        <div style="margin-bottom:12px;">
          <span style="font-weight:600;color:#333;font-size:13px;">Responsible: </span>
          <span style="color:#666;font-size:13px;">${responsible.name}</span>
        </div>
        <div style="margin-bottom:12px;">
          <span style="font-weight:600;color:#333;font-size:13px;">Plant: </span>
          <span style="color:#666;font-size:13px;">${responsible.plant_name}</span>
        </div>
        <div>
          <span style="font-weight:600;color:#333;font-size:13px;">Department: </span>
          <span style="color:#666;font-size:13px;">${responsible.department_name}</span>
        </div>
      </div>
      
      <!-- Summary Badge -->
      <div style="background:#fff3e0;padding:15px;border-radius:6px;margin-bottom:25px;text-align:center;border:2px solid #ff9800;">
        <span style="font-size:32px;font-weight:700;color:#d32f2f;">${kpisWithActions.length}</span>
        <span style="font-size:14px;color:#666;display:block;margin-top:5px;">KPIs Requiring Corrective Action</span>
      </div>
      
      <!-- KPIs List -->
      <div style="margin-bottom:25px;">
        <h3 style="color:#333;font-size:16px;margin-bottom:15px;border-bottom:2px solid #e0e0e0;padding-bottom:8px;">
          📊 Performance Summary
        </h3>
        
        ${kpisWithActions.map((kpi, index) => `
        <div style="background:#fafafa;border:1px solid #e0e0e0;border-radius:6px;padding:15px;margin-bottom:12px;">
          <div style="display:flex;justify-content:space-between;align-items:start;margin-bottom:10px;">
            <div style="flex:1;">
              <div style="font-weight:600;color:#333;font-size:14px;margin-bottom:5px;">
                ${index + 1}. ${kpi.indicator_title}
              </div>
              ${kpi.indicator_sub_title ? `
              <div style="color:#666;font-size:12px;margin-bottom:8px;">
                ${kpi.indicator_sub_title}
              </div>
              ` : ''}
            </div>
          </div>
          
          <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;background:white;padding:12px;border-radius:4px;">
            <div style="text-align:center;">
              <div style="font-size:10px;color:#666;margin-bottom:4px;text-transform:uppercase;">Current</div>
              <div style="font-size:18px;font-weight:700;color:#d32f2f;">${kpi.value} ${kpi.unit || ''}</div>
            </div>
            <div style="text-align:center;">
              <div style="font-size:10px;color:#666;margin-bottom:4px;text-transform:uppercase;">Target</div>
              <div style="font-size:18px;font-weight:700;color:#4caf50;">${kpi.target} ${kpi.unit || ''}</div>
            </div>
            <div style="text-align:center;">
              <div style="font-size:10px;color:#666;margin-bottom:4px;text-transform:uppercase;">Gap</div>
              <div style="font-size:18px;font-weight:700;color:#ff9800;">
                ${(parseFloat(kpi.target) - parseFloat(kpi.value)).toFixed(2)} ${kpi.unit || ''}
              </div>
            </div>
          </div>
        </div>
        `).join('')}
      </div>
      
      <!-- Action Required Section -->
      <div style="background:#e3f2fd;padding:20px;border-radius:6px;margin-bottom:25px;">
        <h3 style="color:#1976d2;font-size:15px;margin:0 0 12px 0;">📝 What You Need To Do</h3>
        <ul style="margin:0;padding-left:20px;color:#555;font-size:13px;line-height:1.8;">
          <li>Click the button below to access the corrective action form</li>
          <li>Document the root cause for each underperforming KPI</li>
          <li>Describe the implemented solutions</li>
          <li>Provide evidence of improvement actions</li>
          <li><strong>Complete within 24 hours</strong></li>
        </ul>
      </div>
      
      <!-- CTA Button -->
<div style="text-align:center;margin:30px 0;">
  <a href="http://localhost:5000/corrective-actions-bulk?responsible_id=${responsible.responsible_id}&week=${week}"
     style="display:inline-block;padding:16px 35px;background:#d32f2f;color:white;
            border-radius:8px;text-decoration:none;font-weight:bold;font-size:16px;
            box-shadow:0 4px 10px rgba(211,47,47,0.3);">
    📝 Complete Corrective Actions (${kpisWithActions.length})
  </a>
</div>
      
      <!-- Footer -->
      <div style="margin-top:30px;padding-top:20px;border-top:1px solid #e0e0e0;text-align:center;">
        <p style="font-size:11px;color:#999;margin:0;line-height:1.6;">
          This is an automated alert from AVOCarbon KPI System<br>
          <strong>Week ${week}</strong> • Generated on ${new Date().toLocaleDateString()}<br>
          For assistance, contact: <a href="mailto:administration.STS@avocarbon.com" style="color:#0078D7;">administration.STS@avocarbon.com</a>
        </p>
      </div>
    </div>
  </body>
  </html>
  `;
};

// Send consolidated corrective action email
const sendConsolidatedCorrectiveActionEmail = async (responsibleId, week) => {
  try {
    // Get responsible info
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

    // Get all open corrective actions with KPI details
    const actionsRes = await pool.query(
      `SELECT ca.corrective_action_id, ca.kpi_id, ca.week,
              k.indicator_title, k.indicator_sub_title, k.unit, k.target,
              kv.value
       FROM public.corrective_actions ca
       JOIN public."Kpi" k ON ca.kpi_id = k.kpi_id
       LEFT JOIN public.kpi_values kv ON ca.kpi_id = kv.kpi_id 
         AND kv.responsible_id = ca.responsible_id 
         AND kv.week = ca.week
       WHERE ca.responsible_id = $1 
         AND ca.week = $2 
         AND ca.status = 'Open'
       ORDER BY k.indicator_title`,
      [responsibleId, week]
    );

    if (actionsRes.rows.length === 0) {
      console.log(`No open corrective actions for responsible ${responsibleId}, week ${week}`);
      return null;
    }

    const kpisWithActions = actionsRes.rows;

    const html = generateConsolidatedCorrectiveActionEmail({
      responsible,
      week,
      kpisWithActions
    });

    const transporter = createTransporter();
    const info = await transporter.sendMail({
      from: '"AVOCarbon Quality System" <administration.STS@avocarbon.com>',
      to: responsible.email,
      subject: `⚠️ ${kpisWithActions.length} Corrective Action${kpisWithActions.length > 1 ? 's' : ''} Required - Week ${week}`,
      html,
    });

    console.log(`✅ Consolidated corrective action email sent to ${responsible.email} (${kpisWithActions.length} KPIs)`);
    return info;
  } catch (err) {
    console.error(`❌ Failed to send consolidated corrective action email:`, err.message);
    throw err;
  }
};

// ---------- Redirect handler with auto-adjusting target ----------
// ---------- Redirect handler with auto-adjusting target (TEXT version) ----------
app.get("/redirect", async (req, res) => {
  try {
    const { responsible_id, week, ...values } = req.query;
    const kpiValues = Object.entries(values)
      .filter(([key]) => key.startsWith("value_"))
      .map(([key, val]) => ({
        kpi_values_id: key.split("_")[1],
        value: val,
      }));

    // Arrays to collect updates
    const targetUpdates = [];
    let hasCorrectiveActions = false;

    for (let item of kpiValues) {
      const oldRes = await pool.query(
        `SELECT value, kpi_id FROM public."kpi_values" WHERE kpi_values_id = $1`,
        [item.kpi_values_id]
      );

      if (oldRes.rows.length) {
        const { value: old_value, kpi_id } = oldRes.rows[0];

        await pool.query(
          `INSERT INTO public.kpi_values_hist26 
          (kpi_values_id, responsible_id, kpi_id, week, old_value, new_value)
          VALUES ($1, $2, $3, $4, $5, $6)`,
          [item.kpi_values_id, responsible_id, kpi_id, week, old_value, item.value]
        );

        await pool.query(
          `UPDATE public."kpi_values" SET value = $1 WHERE kpi_values_id = $2`,
          [item.value, item.kpi_values_id]
        );

        // Get the latest hist_id
        const histRes = await pool.query(
          `SELECT hist_id FROM public.kpi_values_hist26 
           WHERE kpi_values_id = $1 
             AND responsible_id = $2 
             AND kpi_id = $3 
             AND week = $4
           ORDER BY updated_at DESC LIMIT 1`,
          [item.kpi_values_id, responsible_id, kpi_id, week]
        );

        // Check and trigger actions (collects updates but doesn't send emails)
        if (histRes.rows.length > 0) {
          const histId = histRes.rows[0].hist_id;
          const result = await checkAndTriggerCorrectiveActions(responsible_id, kpi_id, week, item.value, histId);

          // Collect target update if it happened
          if (result.targetUpdated && result.updateInfo) {
            targetUpdates.push(result.updateInfo);
          }

          // Check if corrective action was created
          if (!result.targetUpdated) {
            const caCheck = await pool.query(
              `SELECT corrective_action_id FROM public.corrective_actions
               WHERE responsible_id = $1 AND kpi_id = $2 AND week = $3 AND status = 'Open'`,
              [responsible_id, kpi_id, week]
            );
            if (caCheck.rows.length > 0) {
              hasCorrectiveActions = true;
            }
          }
        }
      }
    }

    // ===== SEND ONE CONSOLIDATED EMAIL FOR TARGET UPDATES =====
    if (targetUpdates.length > 0) {
      await sendConsolidatedTargetUpdateEmail(responsible_id, week, targetUpdates);
    }

    // ===== SEND ONE CONSOLIDATED EMAIL FOR CORRECTIVE ACTIONS =====
    if (hasCorrectiveActions) {
      await sendConsolidatedCorrectiveActionEmail(responsible_id, week);
    }

    // Determine success message based on what happened
    let successMessage = `<h1>✅ KPI Submitted Successfully!</h1>`;
    let notifications = [];

    if (targetUpdates.length > 0) {
      notifications.push(`🎯 <strong>${targetUpdates.length} KPI target${targetUpdates.length > 1 ? 's' : ''} updated</strong> - You will receive a consolidated email`);
    }

    if (hasCorrectiveActions) {
      notifications.push(`⚠️ <strong>Corrective actions required</strong> - You will receive a consolidated email`);
    }

    if (notifications.length === 0) {
      notifications.push(`📊 All KPIs are within targets`);
    }

    res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <title>KPI Submitted</title>
        <style>
          body { 
            font-family:'Segoe UI',sans-serif; 
            background:#f4f4f4; 
            display:flex; 
            justify-content:center; 
            align-items:center; 
            height:100vh; 
            margin:0; 
          }
          .success-container {
            background:#fff; 
            padding:40px; 
            border-radius:10px; 
            box-shadow:0 4px 15px rgba(0,0,0,0.1);
            text-align:center;
            max-width:600px;
          }
          h1 { color:#28a745; font-size:28px; margin-bottom:20px; }
          p { font-size:16px; color:#333; margin-bottom:10px; }
          .notifications {
            background:#f8f9fa;
            padding:20px;
            border-radius:8px;
            margin:20px 0;
            text-align:left;
          }
          .notification-item {
            display:flex;
            align-items:center;
            margin:10px 0;
            padding:10px;
            background:white;
            border-radius:6px;
          }
          .notification-icon {
            font-size:20px;
            margin-right:10px;
          }
          .notification-text {
            flex:1;
          }
          .btn {
            display:inline-block;
            padding:12px 25px;
            background:#0078D7;
            color:white;
            text-decoration:none;
            border-radius:6px;
            font-weight:bold;
            margin:5px;
          }
          .btn:hover { background:#005ea6; }
        </style>
      </head>
      <body>
        <div class="success-container">
          ${successMessage}
          <p>Your KPI values for ${week} have been saved.</p>

          <a href="/dashboard?responsible_id=${responsible_id}" class="btn">Go to Dashboard</a>
        </div>
      </body>
      </html>
    `);

  } catch (err) {
    console.error("❌ Error in /redirect:", err.message);
    res.status(500).send(`
      <h2 style="color:red;">❌ Failed to submit KPI values</h2>
      <p>${err.message}</p>
    `);
  }
});
// ---------- Modern Web  page ----------
app.get("/form", async (req, res) => {
  try {
    const { responsible_id, week } = req.query;
    const { responsible, kpis } = await getResponsibleWithKPIs(responsible_id, week);
    if (!kpis.length) return res.send("<p>No KPIs found for this week.</p>");

    res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <title>KPI Form - Week ${week}</title>
        <style>
          body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: #f4f6f9;
            background-image: url('https://images.unsplash.com/photo-1486262715619-67b85e0b08d3?w=1600');
            background-size: cover;
            background-position: center;
            background-attachment: fixed;
            background-repeat: no-repeat;
            padding: 20px;
            margin: 0;
            min-height: 100vh;
          }
          body::before {
            content: '';
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.3);
            z-index: -1;
          }
          .container { 
            max-width: 800px; 
            margin: 0 auto; 
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 8px; 
            box-shadow: 0 4px 20px rgba(0,0,0,0.2);
            overflow: hidden;
          }
          .header { 
            background: #0078D7; 
            color: white; 
            padding: 20px; 
            text-align: center;
          }
          .header h1 { 
            margin: 0; 
            font-size: 24px;
            font-weight: 600;
          }
          .form-section { 
            padding: 30px;
          }
          .info-section {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 6px;
            margin-bottom: 25px;
            border-left: 4px solid #0078D7;
          }
          .info-row {
            display: flex;
            margin-bottom: 15px;
            align-items: center;
          }
          .info-label {
            font-weight: 600;
            color: #333;
            width: 120px;
            font-size: 14px;
          }
          .info-value {
            flex: 1;
            padding: 8px 12px;
            background: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
          }
          .kpi-section {
            margin-top: 30px;
          }
          .kpi-section h3 {
            color: #0078D7;
            margin-bottom: 20px;
            font-size: 18px;
            border-bottom: 2px solid #0078D7;
            padding-bottom: 8px;
          }
          .kpi-card {
            background: #fff;
            border: 1px solid #e1e5e9;
            border-radius: 6px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
          }
          .kpi-title {
            font-weight: 600;
            color: #333;
            margin-bottom: 5px;
            font-size: 15px;
          }
          .kpi-subtitle {
            color: #666;
            font-size: 13px;
            margin-bottom: 10px;
          }
          .kpi-input {
            width: 100%;
            padding: 10px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
            transition: border-color 0.2s;
          }
          .kpi-input:focus {
            border-color: #0078D7;
            outline: none;
            box-shadow: 0 0 0 2px rgba(0,120,215,0.1);
          }
          .submit-btn {
            background: #0078D7;
            color: white;
            border: none;
            padding: 12px 30px;
            border-radius: 4px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: background-color 0.2s;
            display: block;
            width: 100%;
            margin-top: 20px;
          }
          .submit-btn:hover {
            background: #005ea6;
          }
          .unit-label {
            color: #888;
            font-size: 12px;
            margin-top: 5px;
          }

          /* ── Loading Overlay ── */
          .loading-overlay {
            display: none;
            position: fixed;
            inset: 0;
            background: rgba(0, 0, 0, 0.6);
            z-index: 9999;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: 20px;
          }
          .loading-overlay.active {
            display: flex;
          }
          .loading-overlay .spinner {
            width: 56px;
            height: 56px;
            border: 6px solid rgba(255, 255, 255, 0.3);
            border-top-color: #ffffff;
            border-radius: 50%;
            animation: spin 0.9s linear infinite;
          }
          @keyframes spin {
            to { transform: rotate(360deg); }
          }
          .loading-text {
            color: #ffffff;
            font-size: 18px;
            font-weight: 600;
          }
          .loading-sub {
            color: rgba(255, 255, 255, 0.75);
            font-size: 13px;
            margin-top: -14px;
          }
        </style>
      </head>
      <body>

        <!-- Loading Overlay -->
        <div class="loading-overlay" id="loadingOverlay">
          <div class="spinner"></div>
          <div class="loading-text">Submitting KPI Values...</div>
          <div class="loading-sub">Please wait while we process your submission</div>
        </div>

        <div class="container">
          <div class="header">
            <h2 style="color:white;font-size:22px;margin-bottom:5px;">
              KPI Submission - ${week}
            </h2>
          </div>
          
          <div class="form-section">
            <div class="info-section">
              <div class="info-row">
                <div class="info-label">Responsible Name</div>
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
              <div class="info-row">
                <div class="info-label">Week</div>
                <div class="info-value">${week}</div>
              </div>
            </div>

            <div class="kpi-section">
              <h3>KPI Values</h3>
              <form action="/redirect" method="GET" id="kpiForm">
                <input type="hidden" name="responsible_id" value="${responsible_id}" />
                <input type="hidden" name="week" value="${week}" />
                ${kpis.map(kpi => `
                  <div class="kpi-card">
                    <div class="kpi-title">${kpi.subject}</div>
                    ${kpi.indicator_sub_title ? `<div class="kpi-subtitle">${kpi.indicator_sub_title}</div>` : ''}
                    <input 
                      type="text" 
                      name="value_${kpi.kpi_values_id}" 
                      value="${kpi.value || ''}" 
                      placeholder="Enter value" 
                      class="kpi-input"
                    />
                    ${kpi.unit ? `<div class="unit-label">Unit: ${kpi.unit}</div>` : ''}
                  </div>
                `).join('')}
                <button type="submit" class="submit-btn">Submit KPI Values</button>
              </form>
            </div>
          </div>
        </div>

        <script>
          document.getElementById('kpiForm').addEventListener('submit', function () {
            document.getElementById('loadingOverlay').classList.add('active');
          });
        </script>

      </body>
      </html>
    `);
  } catch (err) {
    res.send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});
// ---------- Modern Dashboard by Week ----------
// ---------- Modern Dashboard by Week ----------
app.get("/dashboard", async (req, res) => {
  try {
    const { responsible_id } = req.query;

    // 1️⃣ Fetch responsible info
    const resResp = await pool.query(
      `
      SELECT r.responsible_id, r.name, r.email, r.plant_id, r.department_id,
             p.name AS plant_name, d.name AS department_name
      FROM public."Responsible" r
      JOIN public."Plant" p ON r.plant_id = p.plant_id
      JOIN public."Department" d ON r.department_id = d.department_id
      WHERE r.responsible_id = $1
      `,
      [responsible_id]
    );

    const responsible = resResp.rows[0];
    if (!responsible) throw new Error("Responsible not found");

    // 2️⃣ Fetch ALL historical KPI submissions for this responsible, ALL weeks
    // UPDATED QUERY: Include all KPI fields from the Kpi table
    const kpiRes = await pool.query(
      `
      SELECT DISTINCT ON (h.week, h.kpi_id)
             h.hist_id, h.kpi_values_id, h.new_value as value, h.week, 
             h.kpi_id, h.updated_at,
             k.subject, k.indicator_sub_title, k.unit,
             k.target, k.min, k.max, k.tolerance_type,
             k.up_tolerance, k.low_tolerance, k.frequency,
             k.definition, k.calculation_on, k.target_auto_adjustment
      FROM public.kpi_values_hist26 h
      JOIN public."Kpi" k ON h.kpi_id = k.kpi_id
      WHERE h.responsible_id = $1
      ORDER BY h.week DESC, h.kpi_id ASC, h.updated_at DESC
      `,
      [responsible_id]
    );

    // 3️⃣ Group KPIs by week - FIX: Use Map to preserve order and handle duplicates properly
    const weekMap = new Map();
    kpiRes.rows.forEach(kpi => {
      if (!weekMap.has(kpi.week)) {
        weekMap.set(kpi.week, []);
      }
      weekMap.get(kpi.week).push(kpi);
    });

    // 4️⃣ Build Dashboard HTML
    let html = `
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <title>KPI Dashboard - ${responsible.name}</title>
        <style>
          body {   font-family: 'Segoe UI', sans-serif; 
            background: #f4f6f9;
            /* Automobile industry background */
            background-image: url('https://images.unsplash.com/photo-1486262715619-67b85e0b08d3?w=1600');
            background-size: cover;
            background-position: center;
            background-attachment: fixed;
            background-repeat: no-repeat;
            padding: 20px; 
            margin: 0;
            min-height: 100vh;}
          .container { max-width: 900px; margin: 0 auto; }
          .header { background:#0078D7; color:white; padding:20px; text-align:center; border-radius:8px 8px 0 0; }
          .header h1 { margin:0; font-size:24px; }
          .content { background:#fff; padding:30px; border-radius:0 0 8px 8px; box-shadow:0 2px 10px rgba(0,0,0,0.1); }

          .info-section { background:#f8f9fa; padding:20px; border-radius:6px; margin-bottom:25px; border-left:4px solid #0078D7; }
          .info-row { display:flex; margin-bottom:10px; }
          .info-label { width:120px; font-weight:600; color:#333; }
          .info-value { flex:1; background:white; padding:8px 12px; border:1px solid #ddd; border-radius:4px; }

          .week-section { margin-bottom:30px; border:1px solid #e1e5e9; border-radius:8px; padding:20px; background:#fafbfc; }
          .week-title { color:#0078D7; font-size:20px; margin-bottom:15px; font-weight:600; border-bottom:2px solid #0078D7; padding-bottom:8px; }

          .kpi-card { 
            background:#fff; 
            border:1px solid #e1e5e9; 
            border-radius:6px; 
            padding:15px; 
            margin-bottom:15px; 
            box-shadow:0 1px 3px rgba(0,0,0,0.05); 
          }
          .kpi-title { 
            font-weight:600; 
            color:#333; 
            margin-bottom:5px; 
            font-size: 16px;
          }
          .kpi-subtitle { 
            color:#666; 
            font-size:13px; 
            margin-bottom:10px; 
            font-style: italic;
          }
          .kpi-value { 
            font-size:24px; 
            font-weight:bold; 
            color:#0078D7; 
            margin: 10px 0;
          }
          .kpi-unit { 
            color:#888; 
            font-size:12px; 
            margin-top:5px; 
            display: inline-block;
            background: #f0f7ff;
            padding: 2px 8px;
            border-radius: 4px;
          }
          .kpi-date { 
            color:#999; 
            font-size:11px; 
            margin-top:3px; 
            font-style:italic;
          }
          .no-data { 
            color:#999; 
            font-style:italic; 
            font-size: 18px;
          }
          
          /* KPI Details Styling */
          .kpi-details { 
            margin-top: 15px; 
            padding: 12px; 
            background: #f8f9fa; 
            border-radius: 6px; 
            font-size: 12px;
            border-left: 3px solid #6c757d;
          }
          .kpi-details div { 
            margin-bottom: 6px; 
            display: flex;
            justify-content: space-between;
            border-bottom: 1px dashed #e0e0e0;
            padding-bottom: 4px;
          }
          .kpi-details div:last-child { 
            border-bottom: none;
            margin-bottom: 0;
            padding-bottom: 0;
          }
          .kpi-details strong { 
            color: #495057;
            min-width: 120px;
          }
          .kpi-details span { 
            color: #6c757d;
            text-align: right;
            flex-grow: 1;
          }
          
          .summary { 
            background:#e7f3ff; 
            padding:15px; 
            border-radius:6px; 
            margin-bottom:25px;
            border-left:4px solid #0078D7;
          }
          .summary-text {
            margin:0;
            color:#333;
            font-size:14px;
          }
          
          /* Status indicators */
          .status-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 5px;
          }
          .status-on-target {
            background-color: #28a745;
          }
          .status-near-target {
            background-color: #ffc107;
          }
          .status-below-target {
            background-color: #dc3545;
          }
          
          /* Tolerance styling */
          .tolerance-info {
            background: #fff3cd;
            border: 1px solid #ffeaa7;
            padding: 8px;
            border-radius: 4px;
            margin-top: 8px;
            font-size: 11px;
            color: #856404;
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
              <div class="info-row"><div class="info-label">Responsible</div><div class="info-value">${responsible.name}</div></div>
              <div class="info-row"><div class="info-label">Group</div><div class="info-value">${responsible.plant_name}</div></div>
              <div class="info-row"><div class="info-label">Department</div><div class="info-value">${responsible.department_name}</div></div>
            </div>
            
    `;

    // 5️⃣ Loop through WEEKS using Map
    if (weekMap.size === 0) {
      html += `<div class="no-data">No KPI data available yet.</div>`;
    } else {
      for (const [week, items] of weekMap) {
        html += `
          <div class="week-section">
            <div class="week-title">📅 Week ${week}</div>
        `;

        items.forEach(kpi => {
          const hasValue = kpi.value !== null && kpi.value !== undefined && kpi.value !== '';
          const submittedDate = kpi.updated_at ? new Date(kpi.updated_at).toLocaleString('en-GB', {
            day: '2-digit',
            month: 'short',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
          }) : '';

          // Calculate achievement status if target exists
          let achievementStatus = '';
          let statusClass = '';
          if (kpi.target && hasValue && parseFloat(kpi.target) > 0) {
            const currentValue = parseFloat(kpi.value);
            const targetValue = parseFloat(kpi.target);
            const achievementPercent = (currentValue / targetValue) * 100;

            if (achievementPercent >= 100) {
              statusClass = 'status-on-target';
              achievementStatus = `<span class="status-indicator ${statusClass}"></span> On target`;
            } else if (achievementPercent >= 90) {
              statusClass = 'status-near-target';
              achievementStatus = `<span class="status-indicator ${statusClass}"></span> Near target`;
            } else {
              statusClass = 'status-below-target';
              achievementStatus = `<span class="status-indicator ${statusClass}"></span> Below target`;
            }
          }

          // Calculate tolerance limits if applicable
          let toleranceInfo = '';
          if (kpi.tolerance_type && kpi.target && (kpi.up_tolerance || kpi.low_tolerance)) {
            const targetNum = parseFloat(kpi.target);
            let upperLimit = '';
            let lowerLimit = '';

            if (kpi.tolerance_type === 'Relative') {
              if (kpi.up_tolerance) {
                upperLimit = (targetNum * (1 + parseFloat(kpi.up_tolerance))).toFixed(2);
              }
              if (kpi.low_tolerance) {
                lowerLimit = (targetNum * (1 + parseFloat(kpi.low_tolerance))).toFixed(2);
              }
            } else if (kpi.tolerance_type === 'Absolute') {
              if (kpi.up_tolerance) {
                upperLimit = (targetNum + parseFloat(kpi.up_tolerance)).toFixed(2);
              }
              if (kpi.low_tolerance) {
                lowerLimit = (targetNum + parseFloat(kpi.low_tolerance)).toFixed(2);
              }
            }

            if (upperLimit || lowerLimit) {
              toleranceInfo = `
                <div class="tolerance-info">
                  Tolerance Range: ${lowerLimit ? `${lowerLimit} - ` : ''}${targetNum}${upperLimit ? ` - ${upperLimit}` : ''}
                </div>
              `;
            }
          }

          // Build the KPI card HTML with details
          html += `
            <div class="kpi-card">
              <div class="kpi-title">${kpi.subject}</div>
              ${kpi.indicator_sub_title ? `<div class="kpi-subtitle">${kpi.indicator_sub_title}</div>` : ""}
              ${achievementStatus ? `<div style="font-size: 12px; color: #6c757d; margin-bottom: 5px;">${achievementStatus}</div>` : ''}
              <div class="kpi-value ${!hasValue ? 'no-data' : ''}">${hasValue ? kpi.value : "Not filled yet"}</div>
              ${kpi.unit ? `<span class="kpi-unit">${kpi.unit}</span>` : ""}
              
              <!-- KPI details section -->
              <div class="kpi-details">
                ${kpi.target ? `<div><strong>target:</strong> <span>${kpi.target}</span></div>` : ''}
                ${kpi.min ? `<div><strong>Minimum:</strong> <span>${kpi.min}</span></div>` : ''}
                ${kpi.max ? `<div><strong>Maximum:</strong> <span>${kpi.max}</span></div>` : ''}
                ${kpi.tolerance_type ? `<div><strong>Tolerance Type:</strong> <span>${kpi.tolerance_type}</span></div>` : ''}
                ${kpi.up_tolerance ? `<div><strong>Upper Tolerance:</strong> <span>${kpi.up_tolerance}${kpi.tolerance_type === 'Relative' ? '%' : ''}</span></div>` : ''}
                ${kpi.low_tolerance ? `<div><strong>Lower Tolerance:</strong> <span>${kpi.low_tolerance}${kpi.tolerance_type === 'Relative' ? '%' : ''}</span></div>` : ''}
                ${kpi.calculation_on ? `<div><strong>Calculation Basis:</strong> <span>${kpi.calculation_on}</span></div>` : ''}
                ${kpi.target_auto_adjustment ? `<div><strong>Auto Adjustment:</strong> <span>${kpi.target_auto_adjustment}</span></div>` : ''}
              </div>
              
              ${toleranceInfo}
              
              ${kpi.definition ? `<div style="margin-top: 10px; padding: 8px; background: #e9ecef; border-radius: 4px; font-size: 11px; color: #495057;"><strong>Definition:</strong> ${kpi.definition}</div>` : ''}
              
              ${submittedDate ? `<div class="kpi-date">Last updated: ${submittedDate}</div>` : ""}
            </div>
          `;
        });

        html += `</div>`;
      }
    }

    html += `
          </div>
        </div>
      </body>
      </html>
    `;

    res.send(html);

  } catch (err) {
    console.error("Dashboard error:", err);
    res.status(500).send(`<h2 style="color:red;">Error: ${err.message}</h2>`);
  }
});


app.get("/dashboard-history", async (req, res) => {
  try {
    const { responsible_id, week } = req.query;

    const histRes = await pool.query(
      `
      SELECT h.hist_id, h.kpi_id, h.week, h.old_value, h.new_value, h.updated_at,
             k.subject, k.indicator_sub_title, k.unit
      FROM public.kpi_values_hist26 h
      JOIN public."Kpi" k ON h.kpi_id = k.kpi_id
      WHERE h.responsible_id = $1
      ORDER BY h.updated_at DESC
      `,
      [responsible_id]
    );

    const rows = histRes.rows;

    res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <title>KPI History Dashboard</title>
        <style>
          body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #f4f6f9;
            padding: 20px;
            margin: 0;
          }
          .container {
            max-width: 1000px;
            margin: 0 auto;
            background: #fff;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
          }
          h1 {
            text-align: center;
            color: #0078D7;
            margin-bottom: 30px;
          }
          table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
          }
          th, td {
            padding: 10px 12px;
            border: 1px solid #ddd;
            text-align: center;
          }
          th {
            background: #0078D7;
            color: white;
            font-weight: 600;
          }
          tr:nth-child(even) {
            background: #f8f9fa;
          }
        </style>
      </head>
      <body>
        <div class="container">
          <h1>KPI Value History</h1>
          <table>
            <thead>
              <tr>
                <th>Indicator</th>
                <th>Sub Title</th>
                <th>Week</th>
                <th>Old Value</th>
                <th>New Value</th>
                <th>Unit</th>
                <th>Updated At</th>
              </tr>
            </thead>
            <tbody>
              ${rows
        .map(
          (r) => `
                  <tr>
                    <td>${r.subject}</td>
                    <td>${r.indicator_sub_title || "-"}</td>
                    <td>${r.week}</td>
                    <td>${r.old_value ?? "—"}</td>
                    <td>${r.new_value ?? "—"}</td>
                    <td>${r.unit || ""}</td>
                    <td>${new Date(r.updated_at).toLocaleString()}</td>
                  </tr>`
        )
        .join("")}
            </tbody>
          </table>
        </div>
      </body>
      </html>
    `);
  } catch (err) {
    console.error("❌ Error loading dashboard-history:", err.message);
    res.send(`<p style="color:red;">Error: ${err.message}</p>`);
  }
});


// ---------- Send KPI email ----------
const sendKPIEmail = async (responsibleId, week) => {
  try {
    const { responsible } = await getResponsibleWithKPIs(responsibleId, week);
    const html = generateEmailHtml({ responsible, week });
    const transporter = createTransporter();
    const info = await transporter.sendMail({
      from: '"Administration STS" <administration.STS@avocarbon.com>',
      to: responsible.email,
      subject: `KPI Form for ${responsible.name} - ${week}`,
      html,
    });
    console.log(`✅ Email sent to ${responsible.email}: ${info.messageId}`);
  } catch (err) {
    console.error(`❌ Failed to send email to responsible ID ${responsibleId}:`, err.message);
  }
};

// ✅ HELPER: Format numbers - remove .00 for whole numbers
const formatNumber = (num) => {
  const numValue = parseFloat(num);

  // Check if it's a whole number (no decimal part)
  if (Number.isInteger(numValue)) {
    return numValue.toString();
  }

  // Check if it's effectively a whole number (like 3.00, 900.00)
  if (Math.abs(numValue - Math.round(numValue)) < 0.0001) {
    return Math.round(numValue).toString();
  }

  // For numbers with actual decimal values, show 1 decimal place
  return numValue.toFixed(1);
};
// ---------- Generate HTML/CSS Charts ----------
// Complete corrected generateVerticalBarChart function with proper target line positioning
const generateVerticalBarChart = (chartData) => {
  const {
    title,
    subtitle,
    unit,
    data,
    weekLabels,
    currentWeek,
    stats,
    target,
    min,
    max,
    tolerance_type,
    up_tolerance,
    low_tolerance,
    upper_tolerance_limit,
    lower_tolerance_limit,
    frequency,
    definition
  } = chartData;

  // ✅ Clean values - handle "None" strings from database
  const cleantarget = target && target !== 'None' && target !== '' && !isNaN(parseFloat(target)) ? parseFloat(target) : null;
  const cleanMin = min && min !== 'None' && min !== '' && !isNaN(parseFloat(min)) ? parseFloat(min) : null;
  const cleanMax = max && max !== 'None' && max !== '' && !isNaN(parseFloat(max)) ? parseFloat(max) : null;

  if (!data || data.length === 0 || data.every(val => val <= 0)) {
    return `
      <table border="0" cellpadding="20" cellspacing="0" width="100%" style="margin: 20px 0; background: white; border-radius: 8px; border: 1px solid #e0e0e0;">
        <tr><td>
          <h3 style="margin: 0; color: #333; font-size: 16px; font-weight: 600;">${title}</h3>
          ${subtitle ? `<p style="margin: 5px 0 0 0; color: #666; font-size: 14px;">${subtitle}</p>` : ''}
          ${definition ? `<p style="margin: 5px 0 0 0; color: #888; font-size: 12px; font-style: italic;">${definition}</p>` : ''}
          <p style="margin: 15px 0; color: #999; font-size: 14px;">No data available</p>
        </td></tr>
      </table>
    `;
  }

  // ✅ IMPROVED: Check if value is extremely small compared to target
  const currentValue = data[data.length - 1] || 0;
  const valueVstargetRatio = cleantarget ? currentValue / cleantarget : 0;
  const isValueExtremelySmall = valueVstargetRatio < 0.01; // Less than 1% of target

  // ✅ HELPER: Format large numbers for display
  const formatLargeNumber = (num) => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(num % 1000000 === 0 ? 0 : 1) + 'M';
    }
    if (num >= 1000) {
      return (num / 1000).toFixed(num % 1000 === 0 ? 0 : 1) + 'k';
    }
    if (num < 1) return num.toFixed(2);
    if (num < 10) return num.toFixed(1);
    return num.toFixed(num % 1 === 0 ? 0 : 1);
  };

  // ✅ UPDATED: Calculate Y-axis scaling - IMPORTANT FIX
  const calculateYAxisScaling = () => {
    // Get all values that need to be displayed
    const allValues = [...data.filter(val => val > 0)];
    if (cleantarget !== null && cleantarget > 0) allValues.push(cleantarget);
    if (cleanMin !== null && cleanMin > 0) allValues.push(cleanMin);
    if (cleanMax !== null && cleanMax > 0) allValues.push(cleanMax);
    if (upper_tolerance_limit) allValues.push(upper_tolerance_limit);
    if (lower_tolerance_limit) allValues.push(lower_tolerance_limit);

    const dataMax = Math.max(...allValues);
    const dataMin = Math.min(...allValues.filter(v => v > 0));

    // ✅ CRITICAL FIX: If value is extremely small compared to target
    // Use a different scaling approach
    if (isValueExtremelySmall && cleantarget && cleantarget > 0) {
      // Option 1: Use value-based scaling instead of target-based
      // Find a reasonable max value that shows both value and target
      const valueBasedMax = Math.max(dataMax * 2, 10); // At least show some bar

      // Create a dual-scale approach
      return {
        maxValue: valueBasedMax,
        interval: valueBasedMax / 4,
        numSteps: 4,
        useDualScale: true,
        targetValue: cleantarget
      };
    }

    // Normal scaling for regular cases
    if (!cleantarget || cleantarget <= 0) {
      const numSteps = 5;
      const interval = Math.max(1, Math.ceil(dataMax / numSteps));
      const maxValue = interval * numSteps;
      return { maxValue, interval, numSteps, useDualScale: false };
    }

    const targetNum = cleantarget;

    // Determine scaling based on target value
    if (targetNum <= 10) {
      return {
        maxValue: Math.max(dataMax, Math.ceil(targetNum * 1.2)),
        interval: Math.ceil(targetNum / 3),
        numSteps: 3,
        useDualScale: false
      };
    }

    if (targetNum <= 50) {
      const rounded = Math.max(dataMax, Math.ceil(targetNum / 5) * 5);
      return {
        maxValue: rounded,
        interval: rounded / 4,
        numSteps: 4,
        useDualScale: false
      };
    }

    if (targetNum <= 200) {
      const rounded = Math.max(dataMax, Math.ceil(targetNum / 20) * 20);
      return {
        maxValue: rounded,
        interval: rounded / 4,
        numSteps: 4,
        useDualScale: false
      };
    }

    if (targetNum <= 1000) {
      const rounded = Math.max(dataMax, Math.ceil(targetNum / 100) * 100);
      return {
        maxValue: rounded,
        interval: rounded / 4,
        numSteps: 4,
        useDualScale: false
      };
    }

    // For very large targets
    const rounded = Math.max(dataMax, Math.ceil(targetNum / 500) * 500);
    return {
      maxValue: rounded,
      interval: rounded / 3,
      numSteps: 3,
      useDualScale: false
    };
  };

  // Calculate scaling
  const scaling = calculateYAxisScaling();
  const { maxValue, interval, numSteps, useDualScale } = scaling;
  const chartHeight = 180;
  const segmentHeight = chartHeight / numSteps;

  // ✅ IMPROVED: Generate Y-axis with special handling for small values
  const generateYAxis = () => {
    let yAxis = '';

    for (let i = numSteps; i >= 0; i--) {
      const value = i * interval;
      let displayValue = formatLargeNumber(value);

      // Special indicator for extremely small values
      if (useDualScale && i === numSteps && cleantarget) {
        displayValue += ` (target: ${formatLargeNumber(cleantarget)})`;
      }

      const tolerance = interval / 2;
      let indicators = '';

      if (cleantarget && Math.abs(value - cleantarget) < tolerance) indicators += ' ';
      if (cleanMax && Math.abs(value - cleanMax) < tolerance) indicators += ' 📈';
      if (cleanMin && Math.abs(value - cleanMin) < tolerance) indicators += ' 📉';

      yAxis += `
        <tr>
          <td height="${segmentHeight}" valign="top" align="right" 
              style="font-size: 10px; color: #666; padding-right: 8px;">
            ${displayValue}${indicators}
          </td>
        </tr>
      `;
    }

    // Add a note for extremely small values
    if (useDualScale) {
      yAxis += `
        <tr>
          <td height="20" valign="top" align="right" 
              style="font-size: 8px; color: #ff9800; padding-right: 8px; font-style: italic;">
            * Value scale ≠ target scale
          </td>
        </tr>
      `;
    }

    return yAxis;
  };

  // ✅ IMPROVED: Calculate segment positions with minimum bar height
  const getSegmentForValue = (value) => {
    if (!value || value <= 0) return -1;
    return Math.round((parseFloat(value) / maxValue) * numSteps);
  };

  const targetSegment = getSegmentForValue(cleantarget);
  const maxSegment = cleanMax !== null ? getSegmentForValue(cleanMax) : -1;
  const minSegment = getSegmentForValue(cleanMin);
  const upperToleranceSegment = getSegmentForValue(upper_tolerance_limit);
  const lowerToleranceSegment = getSegmentForValue(lower_tolerance_limit);

  // ✅ CRITICAL FIX: Calculate bar heights with MINIMUM VISIBLE HEIGHT
  const barSegmentHeights = data.map(value => {
    if (value <= 0) return 0;

    let segmentHeightRatio = (value / maxValue) * numSteps;

    // ENSURE MINIMUM VISIBLE HEIGHT: At least 2 segments for any non-zero value
    if (segmentHeightRatio < 0.5 && value > 0) {
      segmentHeightRatio = 0.5; // Minimum 0.5 segments (will be at least 1px visible)
    }

    return Math.max(1, Math.round(segmentHeightRatio)); // At least 1 segment
  });

  // ✅ UPDATED: Determine bar colors
  const getBarColor = (value) => {
    if (cleantarget && cleantarget > 0) {
      const targetNum = cleantarget;

      if (upper_tolerance_limit && lower_tolerance_limit) {
        if (value >= lower_tolerance_limit && value <= upper_tolerance_limit) {
          return '#28a745';
        } else if (value < lower_tolerance_limit) {
          return '#dc3545';
        } else {
          return '#ff9800';
        }
      }

      if (value >= targetNum) return '#28a745';
      if (value >= targetNum * 0.9) return '#ffc107';
      return '#dc3545';
    }
    return '#2196F3';
  };

  // ✅ UPDATED: Generate chart with visible bars
  const generateChart = () => {
    let chart = '';

    // Start from one segment above max to show values, go down to 0 (X-axis)
    for (let seg = numSteps + 1; seg >= 0; seg--) {
      const hastarget = seg === targetSegment;
      const hasMax = cleanMax !== null && seg === maxSegment && cleanMax !== cleantarget;
      const hasMin = seg === minSegment;
      const hasUpperTolerance = seg === upperToleranceSegment;
      const hasLowerTolerance = seg === lowerToleranceSegment;
      const hasLine = hastarget || hasMax || hasMin || hasUpperTolerance || hasLowerTolerance;

      let lineColor = '';
      let lineLabel = '';
      let lineLabelColor = '';
      let lineStyle = '2px dashed';

      if (hastarget) {
        lineColor = '#28a745';
        lineLabelColor = '#28a745';
        lineLabel = 'target';
      } else if (hasUpperTolerance) {
        lineColor = '#ff9800';
        lineLabelColor = '#ff9800';
        lineStyle = '2px solid';
        lineLabel = 'Upper Tolerance';
      } else if (hasLowerTolerance) {
        lineColor = '#ff9800';
        lineLabelColor = '#ff9800';
        lineStyle = '2px solid';
        lineLabel = 'Lower Tolerance';
      } else if (hasMax) {
        lineColor = '#ff9800';
        lineLabelColor = '#ff9800';
        lineLabel = 'Max';
      } else if (hasMin) {
        lineColor = '#dc3545';
        lineLabelColor = '#dc3545';
        lineLabel = 'Min';
      }

      chart += '<tr>';

      data.forEach((value, idx) => {
        const barHeight = barSegmentHeights[idx];
        const isCurrent = idx === data.length - 1;
        const barColor = getBarColor(value);
        const isExtremelySmall = value > 0 && value < (maxValue * 0.01); // Less than 1% of max

        let cellContent = '';
        let cellBorder = '';

        if (hasLine) {
          cellBorder = `border-top: ${lineStyle} ${lineColor};`;
        }

        // Top area: value label - ALWAYS SHOW for current value
        // Show value label above EVERY bar (not just current/last)
        if (seg === barHeight + 1 && barHeight > 0) {
          const displayVal = formatLargeNumber(value);
          cellContent = `
          <table border="0" cellpadding="2" cellspacing="0" width="100%">
          <tr><td align="center" style="font-size: 10px; font-weight: bold; color: #333;">
           ${displayVal}
           </td></tr>
            </table>
           `;
        }
        else if (seg > 0 && seg <= barHeight) {
          // Ensure minimum bar height for visibility
          const actualBarHeight = Math.max(segmentHeight, 4); // At least 4px
          cellContent = `
            <table border="0" cellpadding="0" cellspacing="0" width="60" align="center">
              <tr>
                <td height="${actualBarHeight}" 
                    style="background-color: ${barColor}; 
                           border: none; 
                           padding: 0; 
                           margin: 0; 
                           font-size: 1px; 
                           line-height: ${actualBarHeight}px;
                           ${isExtremelySmall ? 'border: 1px solid #ff9800;' : ''}">
                  &nbsp;
                </td>
              </tr>
            </table>
          `;

          // Add a dot marker for extremely small values
          if (isExtremelySmall && seg === 1) {
            cellContent += `
              <div style="position: relative; top: -2px; text-align: center;">
                <div style="display: inline-block; width: 6px; height: 6px; background: #ff9800; border-radius: 50%;"></div>
              </div>
            `;
          }
        }
        else if (seg === 0) {
          const w = weekLabels[idx] || `W${idx + 1}`;
          cellContent = `
           <table border="0" cellpadding="2" cellspacing="0" width="100%">
           <tr><td align="center" style="font-size: 10px; color: #666; padding-top: 6px;">
             ${w}
           </td></tr>
           </table>
           `;
        }

        if (hasLine && idx === data.length - 1) {
          cellContent += `
            <table border="0" cellpadding="0" cellspacing="0" width="100%">
              <tr><td align="right" style="font-size: 9px; color: ${lineLabelColor}; font-weight: 600; white-space: nowrap; padding-left: 8px;">
                ${lineLabel}
              </td></tr>
            </table>
          `;
        }

        chart += `
          <td align="center" width="${100 / data.length}%" 
              style="padding: 0 4px; vertical-align: middle; ${cellBorder} 
                     height: ${seg >= 0 ? segmentHeight : 'auto'}px; 
                     line-height: 0; font-size: 0;
                     position: relative;">
            ${cellContent}
          </td>
        `;
      });

      chart += '</tr>';
    }

    return chart;
  };

  // ✅ Calculate achievement
  let targetAchievement = 'N/A';
  let achievementColor = '#dfc54dff';
  if (cleantarget !== null && cleantarget > 0) {
    const currentValue = data[data.length - 1];
    const achievementPercent = ((currentValue / cleantarget) * 100).toFixed(1);
    targetAchievement = `${achievementPercent}%`;

    if (upper_tolerance_limit && lower_tolerance_limit) {
      if (currentValue >= lower_tolerance_limit && currentValue <= upper_tolerance_limit) {
        achievementColor = '#28a745';
      } else if (currentValue < lower_tolerance_limit) {
        achievementColor = '#dc3545';
      } else {
        achievementColor = '#ff9800';
      }
    } else {
      if (parseFloat(achievementPercent) >= 100) {
        achievementColor = '#28a745';
      } else if (parseFloat(achievementPercent) >= 90) {
        achievementColor = '#ffc107';
      } else {
        achievementColor = '#dc3545';
      }
    }
  }

  // Add warning for extremely small values
  let smallValueWarning = '';
  if (isValueExtremelySmall && cleantarget) {
    smallValueWarning = `
      <div style="margin: 10px 0; padding: 10px; background: #fff3cd; border-radius: 6px; border-left: 4px solid #ffc107;">
        <div style="font-size: 12px; color: #856404; font-weight: 600; margin-bottom: 5px;">
          ⚠️ Note: Current value (${formatLargeNumber(currentValue)}) is very small compared to target (${formatLargeNumber(cleantarget)})
        </div>
        <div style="font-size: 11px; color: #856404;">
          Bar height exaggerated for visibility. Achievement: ${targetAchievement}
        </div>
      </div>
    `;
  }

  // Create tolerance info display
  let toleranceInfo = '';
  if (tolerance_type && (up_tolerance || low_tolerance)) {
    toleranceInfo = `
      <div style="margin: 10px 0; padding: 10px; background: #f8f9fa; border-radius: 6px; border-left: 4px solid #6c757d;">
        <div style="font-size: 12px; color: #495057; font-weight: 600; margin-bottom: 5px;">
          Tolerance Settings: ${tolerance_type}
        </div>
        <table border="0" cellpadding="2" cellspacing="2" style="font-size: 11px; color: #6c757d;">
          <tr>
            ${up_tolerance ? `<td>Upper: ${up_tolerance}${tolerance_type === 'Relative' ? '%' : ''}</td>` : ''}
            ${low_tolerance ? `<td>Lower: ${low_tolerance}${tolerance_type === 'Relative' ? '%' : ''}</td>` : ''}
          </tr>
          ${upper_tolerance_limit || lower_tolerance_limit ? `
          <tr>
            ${upper_tolerance_limit ? `<td>Upper Limit: ${formatLargeNumber(upper_tolerance_limit)}</td>` : ''}
            ${lower_tolerance_limit ? `<td>Lower Limit: ${formatLargeNumber(lower_tolerance_limit)}</td>` : ''}
          </tr>
          ` : ''}
        </table>
      </div>
    `;
  }

  // ✅ Return HTML with visible bars
  return `
    <table border="0" cellpadding="0" cellspacing="0" width="100%" style="margin: 20px 0; background: white; border-radius: 8px; border: 1px solid #e0e0e0; font-family: Arial, sans-serif;">
      <tr><td style="padding: 20px;">
        
        <!-- Header -->
        <table border="0" cellpadding="0" cellspacing="0" width="100%" style="margin-bottom: 20px;">
          <tr><td>
            <h3 style="margin: 0; color: #333; font-size: 16px; font-weight: 600;">${title}</h3>
            ${subtitle ? `<p style="margin: 5px 0 0 0; color: #666; font-size: 14px;">${subtitle}</p>` : ''}
            ${definition ? `<p style="margin: 5px 0 0 0; color: #888; font-size: 12px; font-style: italic;">${definition}</p>` : ''}
            ${unit ? `<p style="margin: 5px 0 0 0; color: #888; font-size: 12px;">Unit: ${unit}</p>` : ''}
            ${frequency ? `<p style="margin: 5px 0 0 0; color: #888; font-size: 12px;">Frequency: ${frequency}</p>` : ''}
            
            ${smallValueWarning}
            ${toleranceInfo}
            
            <table border="0" cellpadding="0" cellspacing="0" style="margin-top: 8px;">
              <tr>
                ${cleantarget !== null ? `<td style="color: #28a745; font-size: 12px; font-weight: 600; padding-right: 15px;">🎯 target: ${formatLargeNumber(cleantarget)}</td>` : ''}
                ${cleanMax !== null ? `<td style="color: #ff9800; font-size: 12px; font-weight: 600; padding-right: 15px;">📈 Max: ${formatLargeNumber(cleanMax)}</td>` : ''}
                ${cleanMin !== null ? `<td style="color: #dc3545; font-size: 12px; font-weight: 600;">📉 Min: ${formatLargeNumber(cleanMin)}</td>` : ''}
              </tr>
            </table>
          </td></tr>
        </table>
        
        <!-- Chart -->
        <table border="0" cellpadding="0" cellspacing="0" width="100%">
          <tr>
            <td width="50" valign="top" style="border-right: 2px solid #ccc; padding-right: 10px;">
              <table border="0" cellpadding="0" cellspacing="0" width="100%" style="height: ${chartHeight}px;">
                ${generateYAxis()}
              </table>
            </td>
            <td valign="top" style="padding-left: 10px; border-bottom: 2px solid #ccc;">
              <table border="0" cellpadding="0" cellspacing="0" width="100%">
                ${generateChart()}
              </table>
            </td>
          </tr>
        </table>
        
        <!-- Stats -->
        <table border="0" cellpadding="0" cellspacing="0" width="100%" style="background: #3880c7ff; border-radius: 6px; margin-top: 20px;">
          <tr>
            <td width="20%" align="center" style="border-right: 1px solid #e0e0e0; padding: 10px;">
              <div style="font-size: 11px; color: #666; text-transform: uppercase; margin-bottom: 5px;">CURRENT</div>
                <div style="font-size: 20px; font-weight: 700; color: #4CAF50;">${formatNumber(stats.current)}
              <div style="font-size: 10px; color: #999;">${currentWeek.replace('2026-Week', 'Week ')}</div>
            </td>
            <td width="20%" align="center" style="padding: 10px; background: ${achievementColor}; border-radius: 12px;">
              <div style="font-size: 11px; color: rgba(255,255,255,0.85); text-transform: uppercase; margin-bottom: 5px; font-weight: 600;">target</div>
              <div style="font-size: 20px; font-weight: 700; color: #ffffff;">${targetAchievement}</div>
              <div style="font-size: 10px; color: rgba(255,255,255,0.7);">Achievement</div>
            </td>
            <td width="20%" align="center" style="border-left: 1px solid #e0e0e0; padding: 10px;">
              <div style="font-size: 11px; color: #666; text-transform: uppercase; margin-bottom: 5px;">AVERAGE</div>
              <div style="font-size: 20px; font-weight: 700; color: #ffffff;">${formatNumber(stats.average)}</div>
              <div style="font-size: 10px; color: #999;">${stats.dataPoints || data.length} periods</div>
            </td>
            <td width="20%" align="center" style="border-left: 1px solid #e0e0e0; padding: 10px;">
              <div style="font-size: 11px; color: #666; text-transform: uppercase; margin-bottom: 5px;">TREND</div>
              <div style="font-size: 20px; font-weight: 700; color: ${stats.trend.startsWith('-') ? '#F44336' : '#4CAF50'};">${stats.trend}</div>
              <div style="font-size: 10px; color: #999;">Week over week</div>
            </td>
          </tr>
        </table>
        
        <!-- Legend -->
        <table border="0" cellpadding="0" cellspacing="0" width="100%" style="margin-top: 15px; padding-top: 15px; border-top: 1px solid #f0f0f0;">
          <tr><td align="center">
            <table border="0" cellpadding="8" cellspacing="0">
              <tr>
                ${cleantarget !== null ? `
                  <td><table border="0" cellpadding="0" cellspacing="5"><tr>
                    <td width="20" height="12" style="border-top: 2px dashed #28a745;"></td>
                    <td style="font-size: 11px; color: #666;">target</td>
                  </tr></table></td>
                ` : ''}
                ${upper_tolerance_limit ? `
                  <td><table border="0" cellpadding="0" cellspacing="5"><tr>
                    <td width="20" height="12" style="border-top: 2px solid #ff9800;"></td>
                    <td style="font-size: 11px; color: #666;">Upper Tolerance</td>
                  </tr></table></td>
                ` : ''}
                ${lower_tolerance_limit ? `
                  <td><table border="0" cellpadding="0" cellspacing="5"><tr>
                    <td width="20" height="12" style="border-top: 2px solid #ff9800;"></td>
                    <td style="font-size: 11px; color: #666;">Lower Tolerance</td>
                  </tr></table></td>
                ` : ''}
                ${cleanMax !== null ? `
                  <td><table border="0" cellpadding="0" cellspacing="5"><tr>
                    <td width="20" height="12" style="border-top: 2px dashed #ff9800;"></td>
                    <td style="font-size: 11px; color: #666;">Max</td>
                  </tr></table></td>
                ` : ''}
                ${cleanMin !== null ? `
                  <td><table border="0" cellpadding="0" cellspacing="5"><tr>
                    <td width="20" height="12" style="border-top: 2px dashed #dc3545;"></td>
                    <td style="font-size: 11px; color: #666;">Min</td>
                  </tr></table></td>
                ` : ''}
                ${isValueExtremelySmall ? `
                <td><table border="0" cellpadding="0" cellspacing="5"><tr>
                  <td width="12" height="12" style="background-color: #ff9800; border: 1px solid #ff9800;"></td>
                  <td style="font-size: 11px; color: #666;">Exaggerated for visibility</td>
                </tr></table></td>
                ` : ''}
              </tr>
            </table>
          </td></tr>
        </table>
        
      </td></tr>
    </table>
  `;
};

//parse the new target and previous target 



const generateWeeklyReportData = async (responsibleId, reportWeek) => {
  try {
    // Get historical data from kpi_values_hist26 table with all KPI fields
    const histRes = await pool.query(
      `
      WITH KpiHistory AS (
        SELECT 
          h.kpi_id,
          h.week,
          h.new_value,
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
          ROW_NUMBER() OVER (PARTITION BY h.kpi_id, h.week ORDER BY h.updated_at DESC) as rn
        FROM public.kpi_values_hist26 h
        JOIN public."Kpi" k ON h.kpi_id = k.kpi_id
        WHERE h.responsible_id = $1
          AND h.new_value IS NOT NULL
          AND h.new_value != ''
          AND h.new_value != '0'
      )
      SELECT * FROM KpiHistory 
      WHERE rn = 1
      ORDER BY kpi_id, week ASC
      `,
      [responsibleId]
    );

    if (!histRes.rows.length) {
      console.log(`No historical data found for responsible ${responsibleId}`);
      return null;
    }

    // Group by KPI
    const kpisData = {};
    const weekLabelsSet = new Set();

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
          tolerance_type: row.tolerance_type,
          up_tolerance: row.up_tolerance,
          low_tolerance: row.low_tolerance,
          frequency: row.frequency,
          definition: row.definition,
          weeklyData: new Map()
        };
      }

      // Parse value
      const value = parseFloat(row.new_value);
      if (!isNaN(value) && value > 0) {
        kpisData[kpiId].weeklyData.set(row.week, value);
        weekLabelsSet.add(row.week);
      }
    });

    // Convert week labels set to sorted array
    const weekLabels = Array.from(weekLabelsSet).sort((a, b) => {
      const [yearA, weekA] = a.includes('Week')
        ? [parseInt(a.split('-Week')[0]), parseInt(a.split('-Week')[1])]
        : [0, parseInt(a.replace('Week', ''))];

      const [yearB, weekB] = b.includes('Week')
        ? [parseInt(b.split('-Week')[0]), parseInt(b.split('-Week')[1])]
        : [0, parseInt(b.replace('Week', ''))];

      if (yearA !== yearB) return yearA - yearB;
      return weekA - weekB;
    });

    if (weekLabels.length === 0) {
      console.log(`No valid week data found for responsible ${responsibleId}`);
      return null;
    }

    const charts = [];

    // Process each KPI
    for (const [kpiId, kpiData] of Object.entries(kpisData)) {
      // Prepare data array for all weeks
      const dataPoints = [];

      weekLabels.forEach(week => {
        if (kpiData.weeklyData.has(week)) {
          dataPoints.push(kpiData.weeklyData.get(week));
        } else {
          dataPoints.push(0);
        }
      });

      // Skip if all values are zero
      const hasData = dataPoints.some(val => val > 0);
      if (!hasData) {
        continue;
      }

      // Find current week (latest with data)
      let currentWeek = null;
      let currentValue = 0;
      let previousValue = 0;

      for (let i = weekLabels.length - 1; i >= 0; i--) {
        if (dataPoints[i] > 0) {
          currentWeek = weekLabels[i];
          currentValue = dataPoints[i];

          // Find previous non-zero value for trend calculation
          for (let j = i - 1; j >= 0; j--) {
            if (dataPoints[j] > 0) {
              previousValue = dataPoints[j];
              break;
            }
          }
          break;
        }
      }

      if (!currentWeek) {
        continue;
      }

      // Calculate statistics
      const nonZeroData = dataPoints.filter(val => val > 0);

      if (nonZeroData.length === 0) {
        continue;
      }

      const avg = nonZeroData.reduce((sum, val) => sum + val, 0) / nonZeroData.length;
      const max = Math.max(...nonZeroData);
      const min = Math.min(...nonZeroData);

      // Calculate trend (week-over-week change)
      let trend = '0.0%';
      if (previousValue > 0 && currentValue > 0) {
        const trendValue = ((currentValue - previousValue) / previousValue) * 100;
        trend = (trendValue >= 0 ? '+' : '') + trendValue.toFixed(1) + '%';
      }

      // Calculate target achievement
      let targetAchievement = 'N/A';
      let achievementColor = '#dfc54dff';
      if (kpiData.target !== null && kpiData.target > 0) {
        const achievementPercent = ((currentValue / kpiData.target) * 100).toFixed(1);
        targetAchievement = `${achievementPercent}%`;

        if (parseFloat(achievementPercent) >= 100) {
          achievementColor = '#28a745';
        } else if (parseFloat(achievementPercent) >= 90) {
          achievementColor = '#ffc107';
        } else {
          achievementColor = '#dc3545';
        }
      }

      // Calculate tolerance ranges
      let upperToleranceLimit = null;
      let lowerToleranceLimit = null;

      if (kpiData.tolerance_type === 'Relative' && kpiData.target) {
        const targetNum = parseFloat(kpiData.target);
        if (kpiData.up_tolerance) {
          upperToleranceLimit = targetNum * (1 + parseFloat(kpiData.up_tolerance));
        }
        if (kpiData.low_tolerance) {
          lowerToleranceLimit = targetNum * (1 + parseFloat(kpiData.low_tolerance));
        }
      } else if (kpiData.tolerance_type === 'Absolute' && kpiData.target) {
        const targetNum = parseFloat(kpiData.target);
        if (kpiData.up_tolerance) {
          upperToleranceLimit = targetNum + parseFloat(kpiData.up_tolerance);
        }
        if (kpiData.low_tolerance) {
          lowerToleranceLimit = targetNum + parseFloat(kpiData.low_tolerance);
        }
      }

      const displayWeekLabels = weekLabels.map(week => {
        if (week.includes('2026-Week')) {
          return `W${week.split('-Week')[1]}`;
        } else if (week.includes('Week')) {
          return `W${week.replace('Week', '')}`;
        }
        return week;
      });

      charts.push({
        kpiId: kpiId,
        title: kpiData.title,
        subtitle: kpiData.subtitle,
        unit: kpiData.unit,
        target: kpiData.target,
        min: kpiData.min,
        max: kpiData.max,
        tolerance_type: kpiData.tolerance_type,
        up_tolerance: kpiData.up_tolerance,
        low_tolerance: kpiData.low_tolerance,
        frequency: kpiData.frequency,
        definition: kpiData.definition,
        upper_tolerance_limit: upperToleranceLimit,
        lower_tolerance_limit: lowerToleranceLimit,
        target_achievement: targetAchievement,
        achievement_color: achievementColor,
        data: dataPoints,
        weekLabels: displayWeekLabels,
        fullWeeks: weekLabels,
        currentWeek: currentWeek,
        stats: {
          current: currentValue.toFixed(kpiData.unit === '%' ? 1 : 2),
          previous: previousValue > 0 ? previousValue.toFixed(kpiData.unit === '%' ? 1 : 2) : 'N/A',
          average: avg.toFixed(kpiData.unit === '%' ? 1 : 2),
          max: max.toFixed(kpiData.unit === '%' ? 1 : 2),
          min: min > 0 ? min.toFixed(kpiData.unit === '%' ? 1 : 2) : 'N/A',
          trend: trend,
          dataPoints: nonZeroData.length,
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


// Add this helper function before the generateWeeklyReportEmail function
function getCurrentWeek() {
  const now = new Date();
  const year = now.getFullYear();

  // ISO week number calculation
  const startDate = new Date(year, 0, 1);
  const days = Math.floor((now - startDate) / (24 * 60 * 60 * 1000));
  const weekNumber = Math.ceil((days + startDate.getDay() + 1) / 7);

  return `${year}-Week${weekNumber}`;
}

const generateWeeklyReportEmail = async (responsibleId, reportWeek) => {
  try {
    // Get responsible info
    const resResp = await pool.query(
      `
      SELECT r.responsible_id, r.name, r.email, r.plant_id, r.department_id,
             p.name AS plant_name, d.name AS department_name
      FROM public."Responsible" r
      JOIN public."Plant" p ON r.plant_id = p.plant_id
      JOIN public."Department" d ON r.department_id = d.department_id
      WHERE r.responsible_id = $1
      `,
      [responsibleId]
    );

    const responsible = resResp.rows[0];
    if (!responsible) throw new Error(`Responsible ${responsibleId} not found`);

    console.log(`Generating report for ${responsible.name}, week: ${reportWeek}`);

    // Generate charts data with multiple weeks
    const chartsData = await generateWeeklyReportData(responsibleId, reportWeek);

    let chartsHtml = '';
    let hasData = false;
    let toleranceTypes = new Set();
    let frequencies = new Set();

    if (chartsData && chartsData.length > 0) {
      hasData = true;
      chartsData.forEach(chart => {
        chartsHtml += generateVerticalBarChart(chart);
        if (chart.tolerance_type) toleranceTypes.add(chart.tolerance_type);
        if (chart.frequency) frequencies.add(chart.frequency);
      });
    } else {
      // Check if there's any data at all
      const checkRes = await pool.query(
        `SELECT COUNT(*) FROM public.kpi_values_hist26 WHERE responsible_id = $1`,
        [responsibleId]
      );

      if (parseInt(checkRes.rows[0].count) === 0) {
        chartsHtml = `
          <div style="text-align: center; padding: 60px; background: #f8f9fa; border-radius: 12px; margin-bottom: 20px; border: 2px dashed #dee2e6;">
            <div style="font-size: 48px; color: #adb5bd; margin-bottom: 20px;">📊</div>
            <p style="color: #495057; margin: 0; font-size: 18px; font-weight: 500;">No KPI Data Available</p>
            <p style="color: #6c757d; margin: 10px 0 0 0; font-size: 14px;">
              Start filling your KPI forms to track your performance over time.
            </p>
            <a href="http://localhost:5000/form?responsible_id=${responsible.responsible_id}&week=${reportWeek}"
               style="display: inline-block; margin-top: 20px; padding: 12px 24px; 
                      background: #28a745; color: white; text-decoration: none; 
                      border-radius: 6px; font-weight: 600; font-size: 14px;">
              ✏️ Start Tracking KPIs
            </a>
          </div>
        `;
      } else {
        chartsHtml = `
          <div style="text-align: center; padding: 60px; background: #f8f9fa; border-radius: 12px; margin-bottom: 20px; border: 2px dashed #dee2e6;">
            <div style="font-size: 48px; color: #adb5bd; margin-bottom: 20px;">📈</div>
            <p style="color: #495057; margin: 0; font-size: 18px; font-weight: 500;">Insufficient Data</p>
            <p style="color: #6c757d; margin: 10px 0 0 0; font-size: 14px;">
              Fill your KPI form for week ${reportWeek} to generate performance charts.
            </p>
            <a href="http://localhost:5000/form?responsible_id=${responsible.responsible_id}&week=${reportWeek}"
               style="display: inline-block; margin-top: 20px; padding: 12px 24px; 
                      background: #0078D7; color: white; text-decoration: none; 
                      border-radius: 6px; font-weight: 600; font-size: 14px;">
              ✏️ Fill KPI Form
            </a>
          </div>
        `;
      }
    }

    // Determine frequency and tolerance types for the email
    const frequencyDisplay = frequencies.size > 0 ?
      Array.from(frequencies).join(', ') : 'Monthly';

    const toleranceDisplay = toleranceTypes.size > 0 ?
      Array.from(toleranceTypes).join(', ') : 'N/A';

    const emailHtml = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>KPI Codir Report</title>
  <style>
    @media only screen and (max-width: 600px) {
      .header-buttons {
        flex-direction: column;
        gap: 10px !important;
      }
      .view-history-btn {
        width: 100% !important;
        text-align: center !important;
      }
    }
  </style>
</head>
<body style="margin: 0; padding: 0; font-family: Arial, sans-serif; background: #f4f6f9; line-height: 1.4;">
  <!-- Simple container for Outlook -->
  <table border="0" cellpadding="0" cellspacing="0" width="100%" style="background: #f4f6f9;">
 <tr>
  <td align="center" style="padding: 20px;">
    <!-- Header Content -->
    <table border="0" cellpadding="0" cellspacing="0" width="100%">
      <tr>
        <td style="background: #0078D7; padding: 30px; text-align: center; border-radius: 8px 8px 0 0;">
          <h1 style="margin: 0; color: white; font-size: 24px; font-weight: 600;">📊 KPI Performance Report</h1>
          <p style="margin: 10px 0 0 0; color: rgba(255,255,255,0.9); font-size: 16px;">
            ${reportWeek.replace('2026-Week', 'Week ')} • ${frequencyDisplay} View
          </p>
        </td>
      </tr>
      <tr>
        <td>
          <!-- View History Button Container -->
         <div style="text-align: center; margin-top: 20px;">
  <a href="http://localhost:5000/kpi-trends?responsible_id=${responsible.responsible_id}"
     class="view-history-btn"
     style="
       display: inline-block;
       padding: 12px 24px;
       background-color: #38bdf8; /* sky blue */
       color: white;
       text-decoration: none;
       border-radius: 8px;
       font-weight: 600;
       font-size: 14px;
     ">
    📈 View Kpi History
  </a>
</div>
        </td>
      </tr>
    </table>
  </td>
</tr>
          
          <!-- Responsible Info -->
          <tr>
            <td style="padding: 25px 30px; border-bottom: 1px solid #e9ecef;">
              <table border="0" cellpadding="0" cellspacing="0" width="100%">
                <tr>
                  <td width="33%" align="center" style="padding: 10px;">
                    <div style="font-size: 11px; color: #666; text-transform: uppercase; margin-bottom: 5px;">Responsible</div>
                    <div style="font-size: 16px; font-weight: 600; color: #333;">${responsible.name}</div>
                  </td>
                  <td width="34%" align="center" style="padding: 10px; border-left: 1px solid #e9ecef; border-right: 1px solid #e9ecef;">
                    <div style="font-size: 11px; color: #666; text-transform: uppercase; margin-bottom: 5px;">Group</div>
                    <div style="font-size: 16px; font-weight: 600; color: #333;">${responsible.plant_name}</div>
                  </td>
                  <td width="33%" align="center" style="padding: 10px;">
                    <div style="font-size: 11px; color: #666; text-transform: uppercase; margin-bottom: 5px;">Department</div>
                    <div style="font-size: 16px; font-weight: 600; color: #333;">${responsible.department_name}</div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          
          <!-- KPI Summary Info -->
          ${hasData ? `
          <tr>
            <td style="padding: 20px 30px; background: #f8f9fa; border-bottom: 1px solid #e9ecef;">
              <table border="0" cellpadding="0" cellspacing="0" width="100%">
                <tr>
                  <td align="center" style="color: #666; font-size: 12px;">
                    <p style="margin: 0 0 5px 0;">
                      <strong>${chartsData ? chartsData.length : 0} KPIs Tracked</strong> | 
                      Tolerance Monitoring: ${toleranceDisplay} |
                      Frequency: ${frequencyDisplay}
                    </p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          ` : ''}
          
          <!-- Charts Section -->
          <tr>
            <td style="padding: 30px;">
              ${chartsHtml}
            </td>
          </tr>
          
          <!-- Action Section -->
          <tr>
            <td style="padding: 20px 30px; background: #f8f9fa; border-top: 1px solid #e9ecef;">
              <table border="0" cellpadding="0" cellspacing="0" width="100%">
                <tr>
                  <td align="center">
                    <p style="margin: 15px 0 0 0; color: #666; font-size: 12px; text-align: center;">
                      Click any button above to access different views of your KPI performance
                    </p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          
          <!-- Footer -->
          <tr>
            <td style="padding: 20px 30px; background: #f8f9fa; border-top: 1px solid #e9ecef; border-radius: 0 0 8px 8px;">
              <table border="0" cellpadding="0" cellspacing="0" width="100%">
                <tr>
                  <td align="center" style="color: #666; font-size: 12px;">
                    <p style="margin: 0 0 5px 0;">
                      <strong>AVOCarbon KPI System</strong> | Automated Performance Report
                    </p>
                    <p style="margin: 0; font-size: 11px; color: #999;">
                      Generated on ${new Date().toLocaleDateString('en-GB', {
      weekday: 'long',
      year: 'numeric',
      month: 'long',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    })} • 
                      Contact: <a href="mailto:administration.STS@avocarbon.com" 
                                style="color: #0078D7; text-decoration: none;">administration.STS@avocarbon.com</a>
                    </p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
`;

    // Send email
    const transporter = createTransporter();
    const mailOptions = {
      from: '"AVOCarbon KPI System" <administration.STS@avocarbon.com>',
      to: responsible.email,
      subject: `📊 KPI Performance Trends - ${reportWeek} | ${responsible.name}`,
      html: emailHtml
    };

    const info = await transporter.sendMail(mailOptions);
    console.log(`✅ Weekly report sent to ${responsible.email} for week ${reportWeek}`);

    return info;

  } catch (error) {
    console.error(`❌ Failed to send weekly report to responsible ID ${responsibleId}:`, error.message);
    throw error;
  }
};


// ---------- Schedule weekly email to submit kpi----------
let cronRunning = false;
cron.schedule(
  "48 07 * * *",
  async () => {
    if (cronRunning) return console.log("⏭️ Cron already running, skip...");
    cronRunning = true;

    const forcedWeek = "2026-Week7"; // or dynamically compute current week
    try {
      // Send only to responsibles who actually have KPI records for that week
      const resps = await pool.query(`
        SELECT DISTINCT r.responsible_id
        FROM public."Responsible" r
        JOIN public.kpi_values kv ON kv.responsible_id = r.responsible_id
        WHERE kv.week = $1
      `, [forcedWeek]);

      for (let r of resps.rows) {
        await sendKPIEmail(r.responsible_id, forcedWeek);
      }

      console.log(`✅ KPI emails sent to ${resps.rows.length} responsibles`);
    } catch (err) {
      console.error("❌ Error sending scheduled emails:", err.message);
    } finally {
      cronRunning = false;
    }
  },
  { scheduled: true, timezone: "Africa/Tunis" }
);

// ---------- Schedule Weekly Reports  to send it for each responsible  ----------
let reportCronRunning = false;
cron.schedule(
  "52 09 * * *", // Every MOnday at 9:00 AM
  async () => {
    if (reportCronRunning) {
      console.log("⏭️ Weekly report cron already running, skipping...");
      return;
    }

    reportCronRunning = true;

    try {
      // Calculate current week
      const now = new Date();
      const year = now.getFullYear();

      // Get week number function
      const getWeekNumber = (date) => {
        const d = new Date(date);
        d.setHours(0, 0, 0, 0);
        d.setDate(d.getDate() + 4 - (d.getDay() || 7));
        const yearStart = new Date(d.getFullYear(), 0, 1);
        const weekNo = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
        return weekNo;
      };

      const weekNumber = getWeekNumber(now);
      const currentWeek = `${year}-Week${weekNumber}`;
      const previousWeek = `${year}-Week${weekNumber - 1}`;

      console.log(`Current week: ${currentWeek}, Previous week: ${previousWeek}`);

      // Get all responsibles who have ANY KPI history data
      const resps = await pool.query(`
        SELECT DISTINCT r.responsible_id, r.email, r.name
        FROM public."Responsible" r
        JOIN public.kpi_values_hist26 h ON r.responsible_id = h.responsible_id
        WHERE r.email IS NOT NULL
          AND r.email != ''
        GROUP BY r.responsible_id, r.email, r.name
        HAVING COUNT(h.hist_id) > 0
        ORDER BY r.responsible_id
      `);

      console.log(`📊 Sending weekly reports for week ${previousWeek} to ${resps.rows.length} responsibles...`);

      const results = [];
      for (const [index, resp] of resps.rows.entries()) {
        try {
          await generateWeeklyReportEmail(resp.responsible_id, previousWeek);
          console.log(`  [${index + 1}/${resps.rows.length}] Sent to ${resp.name} (${resp.email})`);
          results.push({
            responsible_id: resp.responsible_id,
            name: resp.name,
            status: 'success'
          });

          // Add delay to avoid rate limiting
          await new Promise(resolve => setTimeout(resolve, 1500));
        } catch (err) {
          console.error(`  [${index + 1}/${resps.rows.length}] Failed for ${resp.name}:`, err.message);
          results.push({
            responsible_id: resp.responsible_id,
            name: resp.name,
            status: 'error',
            message: err.message
          });
        }
      }

      const succeeded = results.filter(r => r.status === 'success').length;
      console.log(`✅ Weekly reports completed. Sent: ${succeeded}/${results.length}`);

      // Log summary
      console.log('\n=== REPORT SUMMARY ===');
      console.log(`Total responsibles: ${results.length}`);
      console.log(`Successfully sent: ${succeeded}`);
      console.log(`Failed: ${results.length - succeeded}`);

      if (results.length - succeeded > 0) {
        const failed = results.filter(r => r.status === 'error');
        console.log('Failed responsibles:');
        failed.forEach(f => console.log(`  - ${f.name}: ${f.message}`));
      }

    } catch (error) {
      console.error("❌ Error in weekly report cron job:", error.message);
    } finally {
      reportCronRunning = false;
    }
  },
  {
    scheduled: true,
    timezone: "Africa/Tunis"
  }
);


// ========== UPDATED QUERY TO GET WEEKLY DATA ==========


// ========== UPDATED CHART FUNCTION WITH WEEKLY BAR CHART ==========
// ========== CORRECTED CHART FUNCTION - HANDLES NULL MAX ==========
// ========== CORRECTED CHART FUNCTION - HANDLES NULL MAX & PROPER SCALING ==========
const createIndividualKPIChart = (kpi) => {
  const color = getDepartmentColor(kpi.department);

  /* ===================== THRESHOLDS - HANDLE "None" STRING ===================== */
  const target = kpi.target && kpi.target !== 'None' && kpi.target !== '' && !isNaN(Number(kpi.target)) ? Number(kpi.target) : null;
  const min = kpi.min && kpi.min !== 'None' && kpi.min !== '' && !isNaN(Number(kpi.min)) ? Number(kpi.min) : null;
  const max = kpi.max && kpi.max !== 'None' && kpi.max !== '' && !isNaN(Number(kpi.max)) ? Number(kpi.max) : null;

  /* ===================== DATA ===================== */
  const weeklyData = kpi.weeklyData || { weeks: [], values: [] };
  const weeks = weeklyData.weeks.slice(0, 12).reverse();
  const values = weeklyData.values.slice(0, 12).reverse();

  if (!values || values.length === 0 || values.every(v => v <= 0)) {
    return `
      <table border="0" cellpadding="15" cellspacing="0" width="100%" style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;margin-bottom:15px">
        <tr><td style="text-align:center;color:#999;font-size:14px;">
          <div style="padding:20px;">
            <div style="font-size:48px;opacity:0.3;">📊</div>
            <div>No data available for ${kpi.subtitle || kpi.title}</div>
          </div>
        </td></tr>
      </table>
    `;
  }

  console.log(`KPI: ${kpi.subtitle}, Values: ${values.join(', ')}, target: ${target}, Min: ${min}, Max: ${max}`);

  /* ===================== TREND ===================== */
  let trendArrow = '→';
  let trendColor = '#f59e0b';

  if (values.length >= 2) {
    const diff = values[values.length - 1] - values[values.length - 2];
    if (diff > 0) {
      trendArrow = '↗';
      trendColor = '#16a34a';
    } else if (diff < 0) {
      trendArrow = '↘';
      trendColor = '#dc2626';
    }
  }

  /* ===================== STATISTICS ===================== */
  const avg = values.reduce((a, b) => a + b, 0) / values.length;
  const currentValue = values[values.length - 1];
  const formattedAverage = avg.toFixed(kpi.unit === '%' ? 1 : 2);

  /* ===================== SCALE - ONLY ADD NON-NULL VALUES ===================== */
  const allValues = [...values.filter(val => val > 0)];
  if (target !== null && target > 0) allValues.push(target);
  if (min !== null && min > 0) allValues.push(min);
  if (max !== null && max > 0) allValues.push(max);

  const dataMax = Math.max(...allValues, 1);

  let interval;
  if (dataMax <= 10) {
    interval = 1;
  } else if (dataMax <= 50) {
    interval = 5;
  } else if (dataMax <= 100) {
    interval = 10;
  } else {
    interval = 20;
  }

  const maxValue = Math.ceil(dataMax / interval) * interval + interval;

  const chartHeight = 180;
  const numSegments = Math.ceil(maxValue);
  const segmentHeight = chartHeight / numSegments;

  console.log(`Chart scale - MaxValue: ${maxValue}, Interval: ${interval}, NumSegments: ${numSegments}, SegmentHeight: ${segmentHeight}`);

  /* ===================== CALCULATE SEGMENT POSITIONS - SKIP NULL MAX ===================== */
  const getSegmentForValue = (value) => {
    if (!value || value <= 0) return -1;
    return Math.round((parseFloat(value) / maxValue) * numSegments);
  };

  const targetSegment = getSegmentForValue(target);
  const maxSegment = max !== null ? getSegmentForValue(max) : -1;
  const minSegment = getSegmentForValue(min);

  const barSegmentHeights = values.map(value => {
    if (value <= 0) return 0;
    return Math.round((value / maxValue) * numSegments);
  });

  console.log(`Bar heights (segments): ${barSegmentHeights.join(', ')}`);
  console.log(`Reference lines - target: ${targetSegment}, Max: ${maxSegment}, Min: ${minSegment}`);

  /* ===================== GENERATE Y-AXIS - SKIP NULL MAX ===================== */
  const generateYAxis = () => {
    const numSteps = Math.ceil(maxValue / interval);
    const stepHeight = chartHeight / numSteps;
    let yAxis = '';

    for (let i = numSteps; i >= 0; i--) {
      const value = i * interval;
      const tolerance = interval / 2;
      let indicators = '';

      if (target !== null && Math.abs(value - target) < tolerance) indicators += ' ';
      if (max !== null && Math.abs(value - max) < tolerance) indicators += ' 📈';
      if (min !== null && Math.abs(value - min) < tolerance) indicators += ' 📉';

      yAxis += `
        <tr>
          <td height="${stepHeight}" valign="top" align="right" style="font-size: 9px; color: #666; padding-right: 8px;">
            ${value}${indicators}
          </td>
        </tr>
      `;
    }
    return yAxis;
  };

  /* ===================== GENERATE CHART - SKIP NULL MAX LINE ===================== */
  const generateChart = () => {
    let chart = '';

    for (let seg = numSegments; seg >= -1; seg--) {
      const hastarget = seg === targetSegment;
      const hasMax = max !== null && seg === maxSegment && max !== target;
      const hasMin = seg === minSegment;
      const hasLine = hastarget || hasMax || hasMin;

      let lineColor = '';
      let lineLabel = '';
      let lineLabelColor = '';

      if (hastarget) {
        lineColor = '#16a34a';
        lineLabelColor = '#16a34a';
        lineLabel = 'target';
      } else if (hasMax) {
        lineColor = '#f97316';
        lineLabelColor = '#f97316';
        lineLabel = 'Max';
      } else if (hasMin) {
        lineColor = '#dc2626';
        lineLabelColor = '#dc2626';
        lineLabel = 'Min';
      }

      chart += '<tr>';

      values.forEach((value, idx) => {
        const barHeight = barSegmentHeights[idx];
        const isCurrent = idx === values.length - 1;

        let cellContent = '';
        let cellBorder = '';

        if (hasLine) {
          cellBorder = `border-top: 2px dashed ${lineColor};`;
        }

        if (seg === barHeight + 1 && barHeight > 0) {
          const displayVal = value >= 100 ? value.toFixed(0) : value.toFixed(kpi.unit === '%' ? 1 : 2);

          cellContent = `
            <table border="0" cellpadding="0" cellspacing="0" width="100%">
              <tr>
                <td align="center" style="font-size: 10px; font-weight: bold; color: #333; padding-bottom: 4px;">
                  ${formatNumber(displayVal)}
                </td>
              </tr>
            </table>
          `;
        }

        // Build bar from bottom (seg 1) up to barHeight
        if (seg > 0 && seg <= barHeight) {
          cellContent += `
           <table border="0" cellpadding="0" cellspacing="0" width="60" align="center">
            <tr>
            <td height="${segmentHeight}" 
            style="background-color: ${barColor}; 
                   border: none; 
                   padding: 0; 
                   margin: 0;">
                   &nbsp;
             </td>
             </tr>
            </table>
              `;
        }

        if (seg === -1) {
          const w = weeks[idx]?.replace('2026-Week', '') || idx + 1;
          cellContent = `
            <table border="0" cellpadding="2" cellspacing="0" width="100%">
              <tr>
                <td align="center" style="font-size: 10px; color: #666; padding-top: 6px;">W${w}</td>
              </tr>
              ${isCurrent ? '<tr><td align="center" style="padding-top:2px;"><div style="width:8px;height:8px;background:#16a34a;border-radius:50%;margin:0 auto;"></div></td></tr>' : ''}
            </table>
          `;
        }

        if (hasLine && idx === values.length - 1) {
          cellContent += `
            <table border="0" cellpadding="0" cellspacing="0" width="100%">
              <tr>
                <td align="right" style="font-size: 9px; color: ${lineLabelColor}; font-weight: 600; white-space: nowrap; padding-left: 8px;">${lineLabel}</td>
              </tr>
            </table>
          `;
        }

        chart += `
          <td align="center" width="${100 / values.length}%" style="padding: 0 4px; vertical-align: middle; ${cellBorder} height: ${seg >= 0 ? segmentHeight : 'auto'}px; line-height: 0; font-size: 0;">
            ${cellContent}
          </td>
        `;
      });

      chart += '</tr>';
    }

    return chart;
  };

  /* ===================== THRESHOLD BADGES ===================== */
  const thresholdBadges = `
    <table border="0" cellpadding="5" cellspacing="0" width="100%" style="margin-top:10px">
      <tr>
        <td align="center" width="33%">
          ${target !== null && target > 0 ? `
            <div style="background:#e8f5e9;color:#2e7d32;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #a5d6a7;display:inline-block;">🎯 target: ${target}</div>
          ` : `
            <div style="background:#f5f5f5;color:#9e9e9e;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #e0e0e0;display:inline-block;">🎯 N/A</div>
          `}
        </td>
        <td align="center" width="33%">
          ${max !== null && max > 0 ? `
            <div style="background:#fff3e0;color:#e65100;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #ffb74d;display:inline-block;">📈 Max: ${max}</div>
          ` : `
            <div style="background:#f5f5f5;color:#9e9e9e;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #e0e0e0;display:inline-block;">📈 N/A</div>
          `}
        </td>
        <td align="center" width="33%">
          ${min !== null && min > 0 ? `
            <div style="background:#ffebee;color:#c62828;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #ef5350;display:inline-block;">📉 Min: ${min}</div>
          ` : `
            <div style="background:#f5f5f5;color:#9e9e9e;padding:5px 10px;border-radius:6px;font-size:10px;font-weight:700;border:1px solid #e0e0e0;display:inline-block;">📉 N/A</div>
          `}
        </td>
      </tr>
    </table>
  `;

  return `
<table border="0" cellpadding="0" cellspacing="0" width="100%" style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;margin-bottom:15px">
  <tr>
    <td style="padding:16px">
      <table border="0" cellpadding="0" cellspacing="0" width="100%">
        <tr>
          <td style="font-weight:700;font-size:14px;color:#333;white-space:nowrap;padding-right:15px;">${kpi.subtitle || kpi.title}</td>
          <td align="center" valign="middle" style="padding:0 15px;">
            <div style="display:inline-block;background:#f1f5f9;border:1px solid #e2e8f0;padding:6px 12px;border-radius:8px;">
              <span style="font-size:11px;color:#475569;font-weight:600;">${kpi.responsible || 'N/A'}</span>
            </div>
          </td>
          <td align="right" width="50" style="padding-left:15px;">
            <div style="background:${trendColor};color:#fff;padding:4px 8px;border-radius:6px;font-weight:700;font-size:12px;display:inline-block;">${trendArrow}</div>
          </td>
        </tr>
      </table>
      <table border="0" cellpadding="5" cellspacing="0" width="100%" style="margin:10px 0">
        <tr>
          <td align="center">
            <span style="background:#8b5cf6;color:white;padding:6px 14px;border-radius:20px;font-size:13px;font-weight:700;display:inline-block;">
              Current: ${currentValue.toFixed(kpi.unit === '%' ? 1 : 2)} ${kpi.unit || ''} | Avg: ${formattedAverage} ${kpi.unit || ''}
            </span>
          </td>
        </tr>
      </table>
      ${thresholdBadges}
      <table border="0" cellpadding="0" cellspacing="0" width="100%" style="margin-top:15px">
        <tr>
          <td width="40" valign="top" style="border-right: 2px solid #e5e7eb; padding-right: 8px;">
            <table border="0" cellpadding="0" cellspacing="0" width="100%" style="height: ${chartHeight}px;">
              ${generateYAxis()}
            </table>
          </td>
          <td valign="top" style="padding-left: 8px; border-bottom: 2px solid #e5e7eb;">
            <table border="0" cellpadding="0" cellspacing="0" width="100%">
              ${generateChart()}
            </table>
          </td>
        </tr>
      </table>
    </td>
  </tr>
</table>
`;
};
// ========== UPDATED HTML GENERATION ==========
const generateManagerReportHtml = (reportData) => {
  const { plant, week, kpisByDepartment, stats } = reportData;

  // Create KPI sections by department
  let kpiSections = '';

  // Get departments sorted alphabetically
  const departments = Object.keys(kpisByDepartment).sort();

  departments.forEach(department => {
    const kpis = kpisByDepartment[department];
    if (kpis.length === 0) return;

    const color = getDepartmentColor(department);

    kpiSections += `
      <!-- Department Section -->
      <table border="0" cellpadding="0" cellspacing="0" width="100%" style="margin-bottom: 40px;">
        <tr>
          <td>
            <!-- Department Header -->
            <table border="0" cellpadding="0" cellspacing="0" width="100%" style="
              margin-bottom: 20px;
              padding-bottom: 10px;
              border-bottom: 3px solid ${color};
            ">
              <tr>
                <td style="padding: 5px 0;">
                  <table border="0" cellpadding="0" cellspacing="0">
                    <tr>
                      <td style="padding: 5px 0;">
                        <table border="0" cellpadding="0" cellspacing="0">
                          <tr>
                            <td style="
                              width: 21px;
                              height: 14px;
                              background: ${color};
                              border-radius: 50%;
                            "></td>
                            <td width="10" style="width: 10px;"></td>
                            <td style="
                              font-size: 20px;
                              font-weight: 700;
                              color: #2c3e50;
                              text-transform: uppercase;
                              letter-spacing: 0.5px;
                              padding-right: 10px;
                            ">${department}</td>
                            <td style="
                              font-size: 12px;
                              color: #6c757d;
                              background: #f8f9fa;
                              padding: 5px 14px;
                              border-radius: 12px;
                              font-weight: 600;
                            ">${kpis.length} KPIs</td>
                          </tr>
                        </table>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
            </table>
            
            <!-- KPI Cards Grid: 3 per row using table -->
            <table border="0" cellpadding="10" cellspacing="0" width="100%">
              ${createKPIRows(kpis)}
            </table>
          </td>
        </tr>
      </table>
    `;
  });

  // Helper function to create KPI rows with 3 cards each
  function createKPIRows(kpis) {
    let rows = '';
    for (let i = 0; i < kpis.length; i += 3) {
      const rowKPIs = kpis.slice(i, i + 3);
      rows += '<tr>';

      rowKPIs.forEach(kpi => {
        rows += `<td width="33%" valign="top" style="padding: 10px;">${createIndividualKPIChart(kpi)}</td>`;
      });

      // Fill empty cells if less than 3 KPIs in row
      const emptyCells = 3 - rowKPIs.length;
      for (let j = 0; j < emptyCells; j++) {
        rows += '<td width="33%" style="padding: 10px;"></td>';
      }

      rows += '</tr>';
    }
    return rows;
  }

  // Return complete HTML
  return `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Plant Weekly KPI Dashboard - ${plant.plant_name}</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    
    body {
      margin: 0;
      padding: 0;
      font-family: 'Inter', Arial, sans-serif;
      background: #f8f9fa;
    }
    
    /* Responsive grid for smaller screens */
    @media (max-width: 1200px) {
      .kpi-grid {
        grid-template-columns: repeat(2, 1fr) !important;
      }
    }
    
    @media (max-width: 768px) {
      .kpi-grid {
        grid-template-columns: 1fr !important;
      }
    }
  </style>
</head>
<body>
  <div style="padding: 30px 20px; max-width: 1400px; margin: 0 auto;">
    <!-- Header -->
    <div style="
      background: white;
      border-radius: 12px;
      padding: 30px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.05);
      margin-bottom: 30px;
    ">
      <div style="
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        margin-bottom: 20px;
      ">
        <div>
          <h1 style="
            margin: 0 0 8px 0;
            font-size: 28px;
            font-weight: 800;
            color: #2c3e50;
            letter-spacing: -0.5px;
          ">
            📊 PLANT WEEKLY KPI DASHBOARD
          </h1>
          <div style="font-size: 14px; color: #6c757d;">
            <strong style="color: #495057;">${plant.plant_name}</strong> • 
            Week: <strong style="color: #495057;">${week.replace('2026-Week', 'W')}</strong> • 
            Manager: <strong style="color: #495057;">${plant.manager || 'N/A'}</strong>
          </div>
        </div>
        
        <div style="text-align: right;">
          <div style="font-size: 13px; color: #6c757d; margin-bottom: 5px;">Updated</div>
          <div style="font-size: 14px; font-weight: 600; color: #495057;">
            ${new Date().toLocaleDateString('en-GB', {
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric'
  })}
          </div>
        </div>
      </div>
      
      <!-- Summary Stats -->
      <div style="
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 15px;
        background: #f8f9fa;
        padding: 20px;
        border-radius: 8px;
      ">
        <div style="text-align: center;">
          <div style="
            font-size: 28px;
            font-weight: 800;
            color: #0078D7;
            margin-bottom: 5px;
          ">${stats.totalDepartments}</div>
          <div style="font-size: 12px; color: #6c757d; font-weight: 500;">Departments</div>
        </div>
        
        <div style="text-align: center;">
          <div style="
            font-size: 28px;
            font-weight: 800;
            color: #28a745;
            margin-bottom: 5px;
          ">${stats.totalKPIs}</div>
          <div style="font-size: 12px; color: #6c757d; font-weight: 500;">Total KPIs</div>
        </div>
        
        <div style="text-align: center;">
          <div style="
            font-size: 28px;
            font-weight: 800;
            color: #6f42c1;
            margin-bottom: 5px;
          ">${week.replace('2026-Week', 'W')}</div>
          <div style="font-size: 12px; color: #6c757d; font-weight: 500;">Current Week</div>
        </div>
      </div>
    </div>
    
    <!-- KPI Sections by Department -->
    <div style="
      background: white;
      padding: 30px;
      border-radius: 12px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.05);
    ">
      ${kpiSections || '<div style="text-align: center; padding: 60px; color: #6c757d;">No KPI data available</div>'}
    </div>
    
    <!-- Footer -->
    <div style="
      background: #2c3e50;
      color: white;
      padding: 25px;
      border-radius: 12px;
      text-align: center;
      margin-top: 30px;
    ">
      <div style="margin-bottom: 20px;">
        <div style="font-size: 16px; font-weight: 600; margin-bottom: 10px;">
          AVOCarbon Plant Analytics
        </div>
        <div style="font-size: 13px; opacity: 0.8;">
          Weekly KPI Performance Monitoring
        </div>
      </div>
      
      <div style="
        display: flex;
        justify-content: center;
        gap: 30px;
        flex-wrap: wrap;
        border-top: 1px solid rgba(255,255,255,0.1);
        padding-top: 20px;
      ">
        <div>
          <div style="font-size: 11px; opacity: 0.6; margin-bottom: 5px;">Contact</div>
          <div style="font-size: 13px;">
            <a href="mailto:${plant.manager_email}" 
               style="color: #4facfe; text-decoration: none;">${plant.manager_email}</a>
          </div>
        </div>
        
        <div>
          <div style="font-size: 11px; opacity: 0.6; margin-bottom: 5px;">Week</div>
          <div style="font-size: 13px; font-weight: 500;">
            ${week.replace('2026-Week', 'Week ')}
          </div>
        </div>
      </div>
    </div>
  </div>
</body>
</html>
  `;
};

// Helper function for department colors (unchanged)
const getDepartmentColor = (departmentName) => {
  // First, handle the extracted department names
  if (departmentName.includes('Sales')) return '#667eea'; // Blue
  if (departmentName.includes('Production')) return '#4facfe'; // Light Blue
  if (departmentName.includes('Quality')) return '#43e97b'; // Green
  if (departmentName.includes('VOH')) return '#909d6fff'; // Pink
  if (departmentName.includes('Engineering')) return '#36a07bff'; // Pink
  if (departmentName.includes('Human resources')) return '#78d69aff'; // Pink
  if (departmentName.includes('Stocks')) return '#6a772aff'; // Pink
  if (departmentName.includes('AR/AP')) return '#96ce25ff'; // Pink
  if (departmentName.includes('Cash')) return '#54591bff'; // Pink
  // Fallback to original mapping
  const colorMap = {
    'Production': '#667eea',
    'Quality': '#f093fb',
    'Maintenance': '#4facfe',
    'Safety': '#43e97b',
    'Operations': '#fa709a',
    'Engineering': '#30cfd0',
    'Supply-chain': '#f6d365',
    'Administration': '#a8edea',
    'Finance': '#f093fb',
    'HR': '#4facfe',
    'IT': '#667eea',
    'Sales': '#43e97b',
    'Other': '#6c757d'
  };

  return colorMap[departmentName] || '#6c757d';
};
// ---------- Modern KPI Trends Dashboard with Charts ----------
app.get("/kpi-trends", async (req, res) => {
  try {
    const { responsible_id } = req.query;

    // 1️⃣ Fetch responsible info
    const resResp = await pool.query(
      `
      SELECT r.responsible_id, r.name, r.email, r.plant_id, r.department_id,
             p.name AS plant_name, d.name AS department_name
      FROM public."Responsible" r
      JOIN public."Plant" p ON r.plant_id = p.plant_id
      JOIN public."Department" d ON r.department_id = d.department_id
      WHERE r.responsible_id = $1
      `,
      [responsible_id]
    );

    const responsible = resResp.rows[0];
    if (!responsible) throw new Error("Responsible not found");

    // 2️⃣ Fetch ALL historical KPI data with target information
    const historyRes = await pool.query(
      `
      WITH KPIHistory AS (
        SELECT 
          h.kpi_id,
          h.week,
          h.new_value,
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
          ROW_NUMBER() OVER (PARTITION BY h.kpi_id, h.week ORDER BY h.updated_at DESC) as rn
        FROM public.kpi_values_hist26 h
        JOIN public."Kpi" k ON h.kpi_id = k.kpi_id
        WHERE h.responsible_id = $1
          AND h.new_value IS NOT NULL
          AND h.new_value != ''
          AND h.new_value ~ '^[0-9.]+$'
      )
      SELECT * FROM KPIHistory WHERE rn = 1
      ORDER BY kpi_id, 
               CAST(SPLIT_PART(week, 'Week', 2) AS INTEGER)
      `,
      [responsible_id]
    );

    if (!historyRes.rows.length) {
      return res.send(`
        <!DOCTYPE html>
        <html>
        <head>
          <meta charset="utf-8">
          <title>KPI Trends - ${responsible.name}</title>
          <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
            
            * {
              margin: 0;
              padding: 0;
              box-sizing: border-box;
            }
            
            body {
              font-family: 'Inter', sans-serif;
              background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
              min-height: 100vh;
              display: flex;
              justify-content: center;
              align-items: center;
              padding: 20px;
            }
            
            .empty-container {
              background: rgba(255, 255, 255, 0.95);
              backdrop-filter: blur(10px);
              border-radius: 24px;
              padding: 60px 40px;
              text-align: center;
              max-width: 500px;
              box-shadow: 0 20px 60px rgba(0,0,0,0.15);
              border: 1px solid rgba(255, 255, 255, 0.2);
            }
            
            .empty-icon {
              font-size: 72px;
              margin-bottom: 30px;
              color: #667eea;
              animation: pulse 2s infinite;
            }
            
            @keyframes pulse {
              0%, 100% { transform: scale(1); }
              50% { transform: scale(1.05); }
            }
            
            h1 {
              color: #2c3e50;
              font-size: 28px;
              font-weight: 700;
              margin-bottom: 15px;
              background: linear-gradient(135deg, #667eea, #764ba2);
              -webkit-background-clip: text;
              -webkit-text-fill-color: transparent;
            }
            
            p {
              color: #666;
              font-size: 16px;
              line-height: 1.6;
              margin-bottom: 30px;
            }
            
            .btn {
              display: inline-flex;
              align-items: center;
              gap: 10px;
              padding: 15px 30px;
              background: linear-gradient(135deg, #667eea, #764ba2);
              color: white;
              text-decoration: none;
              border-radius: 12px;
              font-weight: 600;
              font-size: 16px;
              transition: all 0.3s ease;
              box-shadow: 0 10px 20px rgba(102, 126, 234, 0.3);
            }
            
            .btn:hover {
              transform: translateY(-3px);
              box-shadow: 0 15px 30px rgba(102, 126, 234, 0.4);
            }
          </style>
        </head>
        <body>
          <div class="empty-container">
            <div class="empty-icon">📊</div>
            <h1>No KPI Trends Available</h1>
            <p>Start filling your KPI forms to see beautiful trend visualizations and performance charts.</p>
            <a href="/form?responsible_id=${responsible_id}&week=${getCurrentWeek()}" class="btn">
              ✏️ Start Filling KPIs
            </a>
          </div>
        </body>
        </html>
      `);
    }

    // 3️⃣ Process data for charts
    const kpiData = processKPIChartData(historyRes.rows);

    // 4️⃣ Generate HTML with beautiful charts
    const html = generateTrendsDashboardHTML(responsible, kpiData);

    res.send(html);

  } catch (err) {
    console.error("KPI Trends error:", err);
    res.status(500).send(createErrorHTML(err.message));
  }
});

// Helper function to process KPI data for charts
function processKPIChartData(rows) {
  const kpis = new Map();
  const weeksSet = new Set();

  // Group by KPI
  rows.forEach(row => {
    const kpiId = row.kpi_id;
    if (!kpis.has(kpiId)) {
      kpis.set(kpiId, {
        id: kpiId,
        subject: row.subject,
        subtitle: row.indicator_sub_title,
        unit: row.unit || '',
        target: row.target && row.target !== 'None' ? parseFloat(row.target) : null,
        min: row.min && row.min !== 'None' ? parseFloat(row.min) : null,
        max: row.max && row.max !== 'None' ? parseFloat(row.max) : null,
        definition: row.definition,
        tolerance_type: row.tolerance_type,
        up_tolerance: row.up_tolerance,
        low_tolerance: row.low_tolerance,
        values: [],
        weeks: [],
        colors: []
      });
    }

    const value = parseFloat(row.new_value);
    kpis.get(kpiId).values.push(value);
    kpis.get(kpiId).weeks.push(row.week);

    // Determine color based on value
    const kpi = kpis.get(kpiId);
    let color = '#667eea'; // Default blue

    if (kpi.target !== null) {
      const achievement = (value / kpi.target) * 100;
      if (achievement >= 100) {
        color = '#10b981'; // Green for above target
      } else if (achievement >= 90) {
        color = '#f59e0b'; // Amber for near target
      } else {
        color = '#ef4444'; // Red for below target
      }
    }

    kpis.get(kpiId).colors.push(color);
    weeksSet.add(row.week);
  });

  // Sort weeks
  const allWeeks = Array.from(weeksSet).sort((a, b) => {
    const [yearA, weekA] = a.includes('Week')
      ? [parseInt(a.split('-Week')[0]), parseInt(a.split('-Week')[1])]
      : [0, parseInt(a.replace('Week', ''))];

    const [yearB, weekB] = b.includes('Week')
      ? [parseInt(b.split('-Week')[0]), parseInt(b.split('-Week')[1])]
      : [0, parseInt(b.replace('Week', ''))];

    if (yearA !== yearB) return yearA - yearB;
    return weekA - weekB;
  });

  // Calculate statistics for each KPI
  for (const [kpiId, kpi] of kpis) {
    const values = kpi.values;

    // Basic statistics
    kpi.average = values.reduce((a, b) => a + b, 0) / values.length;
    kpi.maxValue = Math.max(...values);
    kpi.minValue = Math.min(...values);

    // Trend calculation
    if (values.length >= 2) {
      const current = values[values.length - 1];
      const previous = values[values.length - 2];
      kpi.trend = ((current - previous) / previous) * 100;
      kpi.trendIcon = current > previous ? '↗' : current < previous ? '↘' : '→';
      kpi.trendColor = current > previous ? '#10b981' : current < previous ? '#ef4444' : '#6b7280';
    } else {
      kpi.trend = 0;
      kpi.trendIcon = '→';
      kpi.trendColor = '#6b7280';
    }

    // target achievement
    if (kpi.target !== null) {
      const latestValue = values[values.length - 1];
      kpi.achievement = (latestValue / kpi.target) * 100;
      kpi.achievementColor = kpi.achievement >= 100 ? '#10b981' :
        kpi.achievement >= 90 ? '#f59e0b' : '#ef4444';
    }
  }

  return {
    kpis: Array.from(kpis.values()),
    allWeeks: allWeeks,
    totalKPIs: kpis.size,
    totalWeeks: allWeeks.length
  };
}

// Generate beautiful HTML dashboard
function generateTrendsDashboardHTML(responsible, kpiData) {
  const { kpis, allWeeks, totalKPIs, totalWeeks } = kpiData;

  return `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>KPI Trends Dashboard - ${responsible.name}</title>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
    
    :root {
      --primary: #667eea;
      --primary-dark: #5a67d8;
      --secondary: #764ba2;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --gray-50: #f9fafb;
      --gray-100: #f3f4f6;
      --gray-200: #e5e7eb;
      --gray-300: #d1d5db;
      --gray-400: #9ca3af;
      --gray-500: #6b7280;
      --gray-600: #4b5563;
      --gray-700: #374151;
      --gray-800: #1f2937;
      --gray-900: #111827;
    }
    
    * {
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }
    
    body {
      font-family: 'Inter', sans-serif;
      background: linear-gradient(135deg, #f3f4f6 0%, #e5e7eb 100%);
      color: var(--gray-800);
      min-height: 100vh;
      line-height: 1.6;
    }
    
    /* Container */
    .container {
      max-width: 1400px;
      margin: 0 auto;
      padding: 20px;
    }
    
    /* Header */
    .dashboard-header {
      background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%);
      color: white;
      border-radius: 24px;
      padding: 40px;
      margin-bottom: 30px;
      box-shadow: 0 20px 40px rgba(102, 126, 234, 0.15);
      position: relative;
      overflow: hidden;
    }
    
    .dashboard-header::before {
      content: '';
      position: absolute;
      top: -50%;
      right: -50%;
      width: 300px;
      height: 300px;
      background: rgba(255, 255, 255, 0.1);
      border-radius: 50%;
    }
    
    .header-content {
      position: relative;
      z-index: 2;
    }
    
    .header-content h1 {
      font-size: 36px;
      font-weight: 800;
      margin-bottom: 10px;
      display: flex;
      align-items: center;
      gap: 15px;
    }
    
    .header-content .subtitle {
      font-size: 18px;
      opacity: 0.9;
      margin-bottom: 25px;
    }
    
    /* Stats Grid */
    .stats-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 20px;
      margin-top: 30px;
    }
    
    .stat-card {
      background: rgba(255, 255, 255, 0.1);
      backdrop-filter: blur(10px);
      border-radius: 16px;
      padding: 20px;
      border: 1px solid rgba(255, 255, 255, 0.2);
    }
    
    .stat-value {
      font-size: 32px;
      font-weight: 700;
      margin-bottom: 5px;
    }
    
    .stat-label {
      font-size: 14px;
      opacity: 0.8;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    
    /* KPI Grid */
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(600px, 1fr));
      gap: 30px;
      margin-bottom: 40px;
    }
    
    @media (max-width: 1300px) {
      .kpi-grid {
        grid-template-columns: 1fr;
      }
    }
    
    /* KPI Card */
    .kpi-card {
      background: white;
      border-radius: 20px;
      padding: 30px;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.05);
      transition: all 0.3s ease;
      border: 1px solid var(--gray-200);
    }
    
    .kpi-card:hover {
      transform: translateY(-5px);
      box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
    }
    
    .kpi-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      margin-bottom: 25px;
    }
    
    .kpi-title-section h3 {
      font-size: 20px;
      font-weight: 700;
      color: var(--gray-900);
      margin-bottom: 5px;
    }
    
    .kpi-subtitle {
      font-size: 14px;
      color: var(--gray-500);
      margin-bottom: 10px;
    }
    
    .kpi-meta {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    
    .meta-badge {
      padding: 6px 12px;
      border-radius: 20px;
      font-size: 12px;
      font-weight: 600;
      display: inline-flex;
      align-items: center;
      gap: 5px;
    }
    
    .badge-unit {
      background: var(--gray-100);
      color: var(--gray-700);
    }
    
    .badge-trend {
      background: var(--gray-100);
      color: var(--gray-700);
    }
    
    /* Chart Container */
    .chart-container {
      height: 300px;
      margin: 25px 0;
      position: relative;
    }
    
    /* Stats Row */
    .stats-row {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 15px;
      margin-top: 25px;
    }
    
    .stat-box {
      background: var(--gray-50);
      border-radius: 12px;
      padding: 15px;
      text-align: center;
      border: 1px solid var(--gray-200);
    }
    
    .stat-box .label {
      font-size: 12px;
      color: var(--gray-500);
      margin-bottom: 5px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    
    .stat-box .value {
      font-size: 20px;
      font-weight: 700;
      color: var(--gray-900);
    }
    
    .stat-box.target .value {
      color: var(--success);
    }
    
    .stat-box.average .value {
      color: var(--primary);
    }
    
    .stat-box.trend .value {
      color: var(--warning);
    }
    
    .stat-box.achievement .value {
      color: var(--danger);
    }
    
    /* target Indicators */
    .target-indicators {
      display: flex;
      gap: 15px;
      margin-top: 20px;
      flex-wrap: wrap;
    }
    
    .target-item {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 14px;
      color: var(--gray-600);
    }
    
    .target-dot {
      width: 12px;
      height: 12px;
      border-radius: 50%;
    }
    
    .target-dot.target {
      background: var(--success);
    }
    
    .target-dot.min {
      background: var(--danger);
    }
    
    .target-dot.max {
      background: var(--warning);
    }
    
    /* Progress Bar */
    .progress-container {
      margin-top: 20px;
    }
    
    .progress-label {
      display: flex;
      justify-content: space-between;
      margin-bottom: 8px;
      font-size: 14px;
    }
    
    .progress-bar {
      height: 10px;
      background: var(--gray-200);
      border-radius: 5px;
      overflow: hidden;
    }
    
    .progress-fill {
      height: 100%;
      background: linear-gradient(90deg, var(--primary), var(--secondary));
      border-radius: 5px;
      transition: width 1s ease;
    }
    
    /* Navigation */
    .navigation {
      display: flex;
      gap: 15px;
      justify-content: center;
      margin-top: 40px;
      flex-wrap: wrap;
    }
    
    .nav-btn {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 15px 30px;
      background: white;
      color: var(--gray-700);
      text-decoration: none;
      border-radius: 12px;
      font-weight: 600;
      font-size: 16px;
      transition: all 0.3s ease;
      border: 2px solid var(--gray-200);
    }
    
    .nav-btn.primary {
      background: linear-gradient(135deg, var(--primary), var(--secondary));
      color: white;
      border: none;
      box-shadow: 0 10px 20px rgba(102, 126, 234, 0.2);
    }
    
    .nav-btn:hover {
      transform: translateY(-3px);
      box-shadow: 0 15px 30px rgba(0, 0, 0, 0.1);
    }
    
    /* Footer */
    .footer {
      text-align: center;
      margin-top: 50px;
      padding: 30px;
      color: var(--gray-500);
      font-size: 14px;
      border-top: 1px solid var(--gray-200);
    }
    
    /* Animations */
    @keyframes fadeIn {
      from {
        opacity: 0;
        transform: translateY(20px);
      }
      to {
        opacity: 1;
        transform: translateY(0);
      }
    }
    
    .animate-fadeIn {
      animation: fadeIn 0.6s ease-out;
    }
    
    /* Responsive */
    @media (max-width: 768px) {
      .dashboard-header {
        padding: 25px;
      }
      
      .header-content h1 {
        font-size: 28px;
      }
      
      .kpi-grid {
        grid-template-columns: 1fr;
      }
      
      .kpi-card {
        padding: 20px;
      }
      
      .stats-row {
        grid-template-columns: repeat(2, 1fr);
      }
      
      .navigation {
        flex-direction: column;
      }
      
      .nav-btn {
        justify-content: center;
      }
    }
    
    /* Custom Scrollbar */
    ::-webkit-scrollbar {
      width: 8px;
      height: 8px;
    }
    
    ::-webkit-scrollbar-track {
      background: var(--gray-100);
      border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb {
      background: var(--primary);
      border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
      background: var(--primary-dark);
    }
  </style>
</head>
<body style="margin: 0; padding: 0; min-height: 100vh; background: 
    linear-gradient(rgba(0, 0, 0, 0.7), rgba(0, 0, 0, 0.7)),
    url('https://images.unsplash.com/photo-1542744095-fcf48d80b0fd?ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D&auto=format&fit=crop&w=1920&q=80')
    center/cover fixed no-repeat;
    font-family: 'Inter', sans-serif; color: #ffffff;">
  
  <!-- Semi-transparent overlay for better readability -->
  <div style="position: fixed; top: 0; left: 0; right: 0; bottom: 0; 
              background: linear-gradient(135deg, rgba(31, 41, 55, 0.9) 0%, rgba(17, 24, 39, 0.9) 100%);
              z-index: -1;"></div>

  <div class="container animate-fadeIn">
    <!-- Dashboard Header -->
    <header class="dashboard-header" style="background: linear-gradient(135deg, 
        rgba(31, 41, 55, 0.95) 0%, 
        rgba(17, 24, 39, 0.95) 100%); 
        border: 1px solid rgba(255, 255, 255, 0.1);
        backdrop-filter: blur(10px);">
      <div class="header-content">
        <h1 style="color: #ffffff;">
          <i class="fas fa-industry"></i>
          KPI Trends & Analytics
        </h1>
        <p class="subtitle" style="color: rgba(255, 255, 255, 0.8);">
          Visualize your industrial performance metrics across all production weeks
        </p>
        
        <!-- User Info -->
        <div style="display: flex; gap: 30px; margin-top: 20px; flex-wrap: wrap;">
          <div>
            <div style="font-size: 12px; color: rgba(255, 255, 255, 0.6); margin-bottom: 5px;">OPERATOR</div>
            <div style="font-size: 18px; font-weight: 600; color: #ffffff;">${responsible.name}</div>
          </div>
          <div>
            <div style="font-size: 12px; color: rgba(255, 255, 255, 0.6); margin-bottom: 5px;">FACTORY</div>
            <div style="font-size: 18px; font-weight: 600; color: #ffffff;">${responsible.plant_name}</div>
          </div>
          <div>
            <div style="font-size: 12px; color: rgba(255, 255, 255, 0.6); margin-bottom: 5px;">DEPARTMENT</div>
            <div style="font-size: 18px; font-weight: 600; color: #ffffff;">${responsible.department_name}</div>
          </div>
        </div>
        
        <!-- Stats Grid -->
        <div class="stats-grid">
          <div class="stat-card" style="background: rgba(255, 255, 255, 0.1); border: 1px solid rgba(255, 255, 255, 0.2);">
            <div class="stat-value" style="color: #60a5fa;">${totalKPIs}</div>
            <div class="stat-label" style="color: rgba(255, 255, 255, 0.7);">Active KPIs</div>
          </div>
          <div class="stat-card" style="background: rgba(255, 255, 255, 0.1); border: 1px solid rgba(255, 255, 255, 0.2);">
            <div class="stat-value" style="color: #34d399;">${totalWeeks}</div>
            <div class="stat-label" style="color: rgba(255, 255, 255, 0.7);">Production Weeks</div>
          </div>
          <div class="stat-card" style="background: rgba(255, 255, 255, 0.1); border: 1px solid rgba(255, 255, 255, 0.2);">
            <div class="stat-value" style="color: #fbbf24;">${kpis.reduce((acc, kpi) => acc + kpi.values.length, 0)}</div>
            <div class="stat-label" style="color: rgba(255, 255, 255, 0.7);">Data Points</div>
          </div>
          <div class="stat-card" style="background: rgba(255, 255, 255, 0.1); border: 1px solid rgba(255, 255, 255, 0.2);">
            <div class="stat-value" style="color: #f87171;">
              ${kpis.filter(k => k.target !== null).length}
            </div>
            <div class="stat-label" style="color: rgba(255, 255, 255, 0.7);">target KPIs</div>
          </div>
        </div>
      </div>
    </header>
    
    <!-- KPI Charts Grid -->
    <main>
      <div class="kpi-grid">
        ${kpis.map((kpi, index) => generateKPIChartHTML(kpi, index)).join('')}
      </div>
    </main>
    
    <!-- Footer -->
    <footer class="footer" style="background: rgba(0, 0, 0, 0.5); 
            border-top: 1px solid rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);">
      <p style="color: rgba(255, 255, 255, 0.8);">
        <i class="fas fa-industry"></i> AVOCarbon Industrial Analytics • ${new Date().getFullYear()}
      </p>
      <p style="margin-top: 10px; font-size: 12px; color: rgba(255, 255, 255, 0.6);">
        <i class="fas fa-cogs"></i> Real-time production monitoring • 
        <i class="fas fa-chart-line"></i> Industrial performance analytics
      </p>
    </footer>
  </div>
  
  <script>
    // Initialize all charts
    document.addEventListener('DOMContentLoaded', function() {
      ${kpis.map((kpi, index) => initializeChartJS(kpi, index)).join('')}
      
      // Add hover effects
      const kpiCards = document.querySelectorAll('.kpi-card');
      kpiCards.forEach(card => {
        card.addEventListener('mouseenter', function() {
          this.style.transform = 'translateY(-8px)';
          this.style.boxShadow = '0 20px 40px rgba(0, 0, 0, 0.3)';
        });
        
        card.addEventListener('mouseleave', function() {
          this.style.transform = 'translateY(0)';
          this.style.boxShadow = '0 10px 30px rgba(0, 0, 0, 0.2)';
        });
      });
      
      // Add industrial sound effect on button clicks
      const buttons = document.querySelectorAll('.nav-btn');
      buttons.forEach(button => {
        button.addEventListener('click', function(e) {
          // Optional: Add click sound effect
          console.log('Navigation clicked:', this.innerText);
        });
      });
      
      // Add subtle animation to stats cards
      const statCards = document.querySelectorAll('.stat-card');
      statCards.forEach((card, index) => {
        setTimeout(() => {
          card.style.transform = 'translateY(0)';
          card.style.opacity = '1';
        }, index * 100);
      });
    });
  </script>
</body>
</html>
  `;
}

// Generate individual KPI chart HTML
function generateKPIChartHTML(kpi, index) {
  const weekLabels = kpi.weeks.map(w =>
    w.includes('Week') ? `W${w.split('-Week')[1] || w.replace('Week', '')}` : w
  );

  const chartId = `chart-${index}`;
  const achievement = kpi.achievement ? Math.min(kpi.achievement, 100) : 0;

  return `
    <div class="kpi-card">
      <div class="kpi-header">
        <div class="kpi-title-section">
          <h3>${kpi.subject}</h3>
          ${kpi.subtitle ? `<p class="kpi-subtitle">${kpi.subtitle}</p>` : ''}
        </div>
        <div class="kpi-meta">
          ${kpi.unit ? `
            <span class="meta-badge badge-unit">
              <i class="fas fa-ruler"></i> ${kpi.unit}
            </span>
          ` : ''}
          <span class="meta-badge badge-trend" style="color: ${kpi.trendColor}; background: ${kpi.trendColor}15;">
            ${kpi.trendIcon} ${Math.abs(kpi.trend).toFixed(1)}%
          </span>
        </div>
      </div>
      
      <!-- Chart -->
      <div class="chart-container">
        <canvas id="${chartId}"></canvas>
      </div>
      
      <!-- Stats -->
      <div class="stats-row">
        ${kpi.target !== null ? `
          <div class="stat-box target">
            <div class="label">target</div>
            <div class="value">${kpi.target.toFixed(2)}</div>
          </div>
        ` : `
          <div class="stat-box target">
            <div class="label">target</div>
            <div class="value">N/A</div>
          </div>
        `}
        
        <div class="stat-box average">
          <div class="label">Average</div>
          <div class="value">${kpi.average.toFixed(2)}</div>
        </div>
        
        <div class="stat-box trend">
          <div class="label">Trend</div>
          <div class="value" style="color: ${kpi.trendColor};">${kpi.trendIcon} ${Math.abs(kpi.trend).toFixed(1)}%</div>
        </div>
        
        ${kpi.achievement !== undefined ? `
          <div class="stat-box achievement">
            <div class="label">Achievement</div>
            <div class="value" style="color: ${kpi.achievementColor};">${kpi.achievement.toFixed(1)}%</div>
          </div>
        ` : `
          <div class="stat-box achievement">
            <div class="label">Achievement</div>
            <div class="value">N/A</div>
          </div>
        `}
      </div>
      
      <!-- target Indicators -->
      ${kpi.target !== null || kpi.min !== null || kpi.max !== null ? `
        <div class="target-indicators">
          ${kpi.target !== null ? `
            <div class="target-item">
              <div class="target-dot target"></div>
              <span>target: ${kpi.target.toFixed(2)}</span>
            </div>
          ` : ''}
          
          ${kpi.min !== null ? `
            <div class="target-item">
              <div class="target-dot min"></div>
              <span>Min: ${kpi.min.toFixed(2)}</span>
            </div>
          ` : ''}
          
          ${kpi.max !== null ? `
            <div class="target-item">
              <div class="target-dot max"></div>
              <span>Max: ${kpi.max.toFixed(2)}</span>
            </div>
          ` : ''}
        </div>
      ` : ''}
      
      <!-- Progress Bar for Achievement -->
      ${kpi.achievement !== undefined ? `
        <div class="progress-container">
          <div class="progress-label">
            <span>target Achievement</span>
            <span>${kpi.achievement.toFixed(1)}%</span>
          </div>
          <div class="progress-bar">
            <div class="progress-fill" style="width: ${achievement}%; background: ${kpi.achievementColor};"></div>
          </div>
        </div>
      ` : ''}
      
      <!-- KPI Definition -->
      ${kpi.definition ? `
        <div style="margin-top: 20px; padding: 15px; background: #f8fafc; border-radius: 12px; border-left: 4px solid #667eea;">
          <div style="font-size: 12px; color: #64748b; margin-bottom: 5px;">
            <i class="fas fa-info-circle"></i> Definition
          </div>
          <div style="font-size: 14px; color: #475569;">${kpi.definition}</div>
        </div>
      ` : ''}
    </div>
  `;
}

// Initialize Chart.js for each KPI
function initializeChartJS(kpi, index) {
  const chartId = `chart-${index}`;
  const weekLabels = kpi.weeks.map(w =>
    w.includes('Week') ? `W${w.split('-Week')[1] || w.replace('Week', '')}` : w
  );

  // Create gradient for chart
  const gradient = `linear-gradient(135deg, ${kpi.colors[0] || '#667eea'}, ${kpi.colors[kpi.colors.length - 1] || '#764ba2'})`;

  return `
    // Chart ${index}
    const ctx${index} = document.getElementById('${chartId}').getContext('2d');
    
    // Create gradient
    const gradient${index} = ctx${index}.createLinearGradient(0, 0, 0, 300);
    gradient${index}.addColorStop(0, '${kpi.colors[0] || '#667eea'}80');
    gradient${index}.addColorStop(1, '${kpi.colors[0] || '#667eea'}20');
    
    new Chart(ctx${index}, {
      type: 'line',
      data: {
        labels: ${JSON.stringify(weekLabels)},
        datasets: [{
          label: '${kpi.subject}',
          data: ${JSON.stringify(kpi.values)},
          borderColor: '${kpi.colors[0] || '#667eea'}',
          backgroundColor: gradient${index},
          borderWidth: 3,
          fill: true,
          tension: 0.4,
          pointBackgroundColor: function(context) {
            const index = context.dataIndex;
            return '${kpi.colors[index] || '#667eea'}';
          },
          pointBorderColor: '#ffffff',
          pointBorderWidth: 2,
          pointRadius: 6,
          pointHoverRadius: 8
        }${kpi.target !== null ? `,
        {
          label: 'target',
          data: Array(${kpi.values.length}).fill(${kpi.target}),
          borderColor: '#10b981',
          borderWidth: 2,
          borderDash: [5, 5],
          fill: false,
          pointRadius: 0
        }` : ''}${kpi.min !== null ? `,
        {
          label: 'Minimum',
          data: Array(${kpi.values.length}).fill(${kpi.min}),
          borderColor: '#ef4444',
          borderWidth: 1,
          borderDash: [3, 3],
          fill: false,
          pointRadius: 0
        }` : ''}${kpi.max !== null ? `,
        {
          label: 'Maximum',
          data: Array(${kpi.values.length}).fill(${kpi.max}),
          borderColor: '#f59e0b',
          borderWidth: 1,
          borderDash: [3, 3],
          fill: false,
          pointRadius: 0
        }` : ''}]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: 'top',
            labels: {
              font: {
                size: 12
              },
              padding: 20,
              usePointStyle: true
            }
          },
          tooltip: {
            backgroundColor: 'rgba(31, 41, 55, 0.9)',
            titleFont: { size: 14 },
            bodyFont: { size: 13 },
            padding: 12,
            cornerRadius: 8,
            displayColors: false,
            callbacks: {
              label: function(context) {
                return '${kpi.unit ? kpi.unit + ': ' : ''}' + context.parsed.y.toFixed(2);
              }
            }
          }
        },
        scales: {
          x: {
            grid: {
              color: 'rgba(0, 0, 0, 0.05)'
            },
            ticks: {
              font: {
                size: 11
              }
            }
          },
          y: {
            beginAtZero: true,
            grid: {
              color: 'rgba(0, 0, 0, 0.05)'
            },
            ticks: {
              font: {
                size: 11
              },
              callback: function(value) {
                return value + '${kpi.unit ? ' ' + kpi.unit : ''}';
              }
            }
          }
        },
        interaction: {
          intersect: false,
          mode: 'index'
        },
        animations: {
          tension: {
            duration: 1000,
            easing: 'linear'
          }
        }
      }
    });
  `;
}

// Error HTML
function createErrorHTML(message) {
  return `
  <!DOCTYPE html>
  <html>
  <head>
    <meta charset="utf-8">
    <title>Error - KPI Trends</title>
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');
      
      body {
        font-family: 'Inter', sans-serif;
        background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
        min-height: 100vh;
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 20px;
        margin: 0;
      }
      
      .error-container {
        background: rgba(255, 255, 255, 0.95);
        backdrop-filter: blur(10px);
        border-radius: 24px;
        padding: 50px 40px;
        text-align: center;
        max-width: 500px;
        box-shadow: 0 20px 60px rgba(220, 38, 38, 0.2);
      }
      
      .error-icon {
        font-size: 72px;
        margin-bottom: 30px;
        color: #ef4444;
      }
      
      h1 {
        color: #1f2937;
        font-size: 28px;
        font-weight: 700;
        margin-bottom: 15px;
      }
      
      .error-message {
        color: #6b7280;
        font-size: 16px;
        line-height: 1.6;
        margin-bottom: 30px;
        padding: 15px;
        background: #fef2f2;
        border-radius: 12px;
        border-left: 4px solid #ef4444;
      }
      
      .btn {
        display: inline-flex;
        align-items: center;
        gap: 10px;
        padding: 15px 30px;
        background: linear-gradient(135deg, #ef4444, #dc2626);
        color: white;
        text-decoration: none;
        border-radius: 12px;
        font-weight: 600;
        font-size: 16px;
        transition: all 0.3s ease;
      }
      
      .btn:hover {
        transform: translateY(-3px);
        box-shadow: 0 15px 30px rgba(220, 38, 38, 0.3);
      }
    </style>
  </head>
  <body>
    <div class="error-container">
      <div class="error-icon">❌</div>
      <h1>Error Loading KPI Trends</h1>
      <div class="error-message">${message}</div>
      <a href="/" class="btn">
        <i class="fas fa-home"></i>
        Return to Home
      </a>
    </div>
  </body>
  </html>
  `;
}


// ---------- Start server ----------
app.listen(port, () => console.log(`🚀 Server running on port ${port}`));
