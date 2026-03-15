-- Ramp credit card transactions from Odoo (replaces CSV import).
-- Filtered by: project code match OR CAPEX GL account category.
-- {project_code_filters} placeholder is injected at pipeline runtime from settings.

WITH project_scope AS (
  SELECT id AS project_id, name AS project_name
  FROM `{odoo_source}.account_analytic_account`
  WHERE {project_code_filters}
),

capex_accounts AS (
  SELECT id AS account_id
  FROM `{odoo_source}.account_account`
  WHERE LOWER(name) LIKE '%construction in process%'
     OR LOWER(name) LIKE '%machinery >%'
     OR LOWER(name) LIKE '%machinery <%'
     OR LOWER(name) LIKE '%furniture%>%2k%'
     OR LOWER(name) LIKE '%furniture%<%2k%'
     OR LOWER(name) LIKE '%r&d materials%'
     OR LOWER(name) LIKE '%r&d services%'
     OR LOWER(name) LIKE '%r&d testing equipment%'
     OR LOWER(name) LIKE '%r&d shipping%'
     OR LOWER(name) LIKE '%tooling & consumables%'
     OR LOWER(name) LIKE '%shop tooling%'
     OR LOWER(name) LIKE '%deployment tooling%'
     OR LOWER(name) LIKE '%it equipment >%'
     OR LOWER(name) LIKE '%it equipment <%'
     OR LOWER(name) LIKE '%office equipment%'
     OR LOWER(name) LIKE '%software & apps%'
     OR LOWER(name) LIKE '%g&a shipping%'
     OR LOWER(name) LIKE '%repair & maintenance%'
     OR LOWER(name) LIKE '%inbound production shipping%'
),

ramp_moves AS (
  SELECT
    am.id AS move_id,
    am.name AS bill_number,
    am.x_para_ramp_external_id AS ramp_external_id,
    am.partner_id AS vendor_partner_id,
    am.invoice_partner_display_name AS vendor_name,
    am.state AS bill_state,
    am.payment_state,
    am.date AS posting_date,
    am.invoice_date,
    am.invoice_date_due,
    am.create_uid,
    CAST(am.amount_total_signed AS BIGNUMERIC) AS amount_total_signed,
    CAST(am.amount_residual_signed AS BIGNUMERIC) AS amount_residual_signed,
    am.ref AS move_ref,
    am.create_date
  FROM `{odoo_source}.account_move` am
  WHERE am.x_para_ramp_external_id IS NOT NULL
    AND IFNULL(am._fivetran_deleted, FALSE) = FALSE
    AND am.move_type IN ('in_invoice', 'in_refund')
    AND am.state = 'posted'
    AND am.invoice_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 MONTH)
),

ramp_lines AS (
  SELECT
    aml.id AS line_id,
    aml.move_id,
    aml.sequence,
    aml.name AS line_description,
    aml.ref AS line_ref,
    aml.product_id,
    aml.account_id,
    CAST(aml.quantity AS BIGNUMERIC) AS quantity,
    CAST(aml.price_unit AS BIGNUMERIC) AS price_unit,
    CAST(aml.price_subtotal AS BIGNUMERIC) AS price_subtotal,
    CAST(aml.price_total AS BIGNUMERIC) AS price_total,
    aml.analytic_account_project_id,
    aml.analytic_account_department_id,
    aml.purchase_line_id
  FROM `{odoo_source}.account_move_line` aml
  WHERE aml.display_type = 'product'
    AND IFNULL(aml._fivetran_deleted, FALSE) = FALSE
    AND (
      aml.analytic_account_project_id IN (SELECT project_id FROM project_scope)
      OR aml.account_id IN (SELECT account_id FROM capex_accounts)
    )
)

SELECT
  rm.bill_number,
  rm.ramp_external_id,
  rm.vendor_name,
  rm.vendor_partner_id,
  rm.bill_state,
  rm.payment_state,
  rm.posting_date,
  rm.invoice_date,
  rm.invoice_date_due,
  rm.move_ref,

  ABS(rm.amount_total_signed) AS bill_amount_total,
  ABS(rm.amount_residual_signed) AS bill_amount_open,
  ABS(rm.amount_total_signed) - ABS(rm.amount_residual_signed) AS bill_amount_paid,

  rl.line_id,
  rl.line_description,
  rl.line_ref,
  rl.product_id,
  ABS(rl.quantity) AS product_qty,
  ABS(rl.price_unit) AS price_unit,
  ABS(rl.price_subtotal) AS price_subtotal,
  ABS(rl.price_total) AS price_total,

  rl.analytic_account_project_id AS project_analytic_id,
  aaa.name AS project_name,

  aa.name AS gl_account_name,
  dept.name AS department_name,

  creator_p.name AS created_by_name,
  rm.create_uid,
  rm.create_date,

  rl.purchase_line_id

FROM ramp_moves rm
JOIN ramp_lines rl ON rl.move_id = rm.move_id
LEFT JOIN `{odoo_source}.account_analytic_account` aaa
  ON aaa.id = rl.analytic_account_project_id
LEFT JOIN `{odoo_source}.account_account` aa
  ON aa.id = rl.account_id
LEFT JOIN `{odoo_source}.account_analytic_account` dept
  ON dept.id = rl.analytic_account_department_id
LEFT JOIN `{odoo_source}.res_users` creator_u ON rm.create_uid = creator_u.id
LEFT JOIN `{odoo_source}.res_partner` creator_p ON creator_u.partner_id = creator_p.id

ORDER BY rm.invoice_date DESC, rm.move_id, rl.sequence
