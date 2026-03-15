-- Payment details: traces PO lines through bills to actual payments.
-- Also pulls payment term definitions for milestone extraction.
-- Scoped to the same creator list as the main PO query ({creator_names} from settings).
-- Dataset: {odoo_source}

WITH creators AS (
  SELECT u.id AS user_id
  FROM `{odoo_source}.res_users` u
  JOIN `{odoo_source}.res_partner` p ON u.partner_id = p.id
  WHERE LOWER(TRIM(p.name)) IN ({creator_names})
),

po_scope AS (
  SELECT po.id AS po_id, po.name AS po_number
  FROM `{odoo_source}.purchase_order` po
  WHERE po.create_uid IN (SELECT user_id FROM creators)
    AND po.date_order >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 MONTH)
    AND po.state IN ('purchase', 'sent', 'done')
),

-- Bill links: PO line -> account_move_line -> account_move (vendor bill)
bill_detail AS (
  SELECT
    aml.purchase_line_id AS po_line_id,
    am.id AS bill_id,
    am.name AS bill_number,
    am.state AS bill_state,
    am.payment_state,
    am.date AS bill_posting_date,
    am.invoice_date AS bill_invoice_date,
    am.invoice_date_due AS bill_due_date,
    CAST(am.amount_total_signed AS BIGNUMERIC) AS bill_total_signed,
    CAST(am.amount_residual_signed AS BIGNUMERIC) AS bill_residual_signed
  FROM `{odoo_source}.account_move_line` aml
  JOIN `{odoo_source}.account_move` am
    ON am.id = aml.move_id
  WHERE aml.purchase_line_id IS NOT NULL
    AND am.move_type IN ('in_invoice', 'in_refund')
    AND IFNULL(am._fivetran_deleted, FALSE) = FALSE
),

-- Payments linked to bills via reconciliation
-- account_partial_reconcile links debit_move_id (bill line) to credit_move_id (payment line)
payment_links AS (
  SELECT
    apr.debit_move_id,
    apr.credit_move_id,
    apr.amount,
    pay_move.date AS payment_date,
    pay_move.name AS payment_ref,
    pay_aml.move_id AS payment_move_id
  FROM `{odoo_source}.account_partial_reconcile` apr
  JOIN `{odoo_source}.account_move_line` bill_aml
    ON bill_aml.id = apr.debit_move_id
  JOIN `{odoo_source}.account_move_line` pay_aml
    ON pay_aml.id = apr.credit_move_id
  JOIN `{odoo_source}.account_move` pay_move
    ON pay_move.id = pay_aml.move_id
  WHERE IFNULL(apr._fivetran_deleted, FALSE) = FALSE
    AND pay_move.move_type IN ('entry')
),

-- Payment terms: definitions from purchase_order -> account_payment_term
-- Note: column names vary by Odoo version; nb_days or days may be used.
payment_terms AS (
  SELECT
    apt.id AS term_id,
    apt.name AS term_name,
    aptl.value AS term_type,
    aptl.value_amount AS term_pct,
    COALESCE(aptl.nb_days, 0) AS term_days
  FROM `{odoo_source}.account_payment_term` apt
  LEFT JOIN `{odoo_source}.account_payment_term_line` aptl
    ON aptl.payment_id = apt.id
  WHERE IFNULL(apt._fivetran_deleted, FALSE) = FALSE
),

-- Combine: PO -> PO line -> bill -> payments + payment terms
combined AS (
  SELECT
    ps.po_number,
    pol.id AS po_line_id,
    pol.name AS line_description,
    pol.price_subtotal AS line_amount,
    po.date_order,
    po.payment_term_id,
    pt_name.term_name AS payment_term_name,

    bd.bill_id,
    bd.bill_number,
    bd.bill_state,
    bd.payment_state AS bill_payment_state,
    bd.bill_posting_date,
    bd.bill_invoice_date,
    bd.bill_due_date,
    ABS(bd.bill_total_signed) AS bill_amount,
    ABS(bd.bill_residual_signed) AS bill_open_amount,

    pl.payment_date,
    pl.payment_ref,
    pl.amount AS payment_amount,

    v.name AS vendor_name

  FROM po_scope ps
  JOIN `{odoo_source}.purchase_order` po ON po.id = ps.po_id
  JOIN `{odoo_source}.purchase_order_line` pol ON pol.order_id = po.id
  LEFT JOIN `{odoo_source}.res_partner` v ON po.partner_id = v.id
  LEFT JOIN bill_detail bd ON bd.po_line_id = pol.id
  LEFT JOIN `{odoo_source}.account_move_line` bill_aml
    ON bill_aml.move_id = bd.bill_id AND bill_aml.purchase_line_id = pol.id
  LEFT JOIN payment_links pl ON pl.debit_move_id = bill_aml.id
  LEFT JOIN (
    SELECT DISTINCT term_id, term_name FROM payment_terms
  ) pt_name ON pt_name.term_id = po.payment_term_id
)

SELECT
  po_number,
  po_line_id,
  vendor_name,
  line_description,
  line_amount,
  date_order,
  payment_term_name,

  bill_id,
  bill_number,
  bill_state,
  bill_payment_state,
  bill_posting_date,
  bill_invoice_date,
  bill_due_date,
  bill_amount,
  bill_open_amount,

  payment_date,
  payment_ref,
  payment_amount,

  DATE_DIFF(payment_date, date_order, DAY) AS days_po_to_payment,
  DATE_DIFF(payment_date, bill_invoice_date, DAY) AS days_bill_to_payment,
  DATE_DIFF(bill_due_date, bill_invoice_date, DAY) AS computed_term_days

FROM combined
WHERE bill_id IS NOT NULL OR payment_date IS NOT NULL
ORDER BY po_number, po_line_id, payment_date
