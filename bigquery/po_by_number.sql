-- Single PO by number: line-level data with Vendor, Project, and key PO fields.
-- Dataset: {odoo_source}
-- Usage: set @po_number in the script or replace 'PO12060' below.

SELECT
  po.id AS po_id,
  po.name AS po_number,
  po.date_order,
  po.date_approve,
  po.state AS po_state,
  po.partner_id AS vendor_partner_id,
  v.name AS vendor_name,
  v.ref AS vendor_ref,
  v.email AS vendor_email,
  pol.id AS line_id,
  pol.sequence AS line_sequence,
  pol.product_id,
  pol.name AS line_description,
  pol.product_qty,
  pol.qty_received,
  pol.product_uom,
  pol.price_unit,
  pol.price_subtotal,
  pol.price_tax,
  pol.price_total,
  pol.date_planned AS line_date_planned,
  pol.analytic_account_project_id AS project_analytic_id,
  aaa.name AS project_name,
  pol.assigned_project_id,
  po.user_id AS responsible_user_id,
  po.create_uid AS created_by_user_id,
  creator_p.name AS created_by_name,
  po.amount_untaxed AS po_amount_untaxed,
  po.amount_tax AS po_amount_tax,
  po.amount_total AS po_amount_total,
  po.currency_id,
  po.company_id,
  po.origin,
  po.incoterm_id,
  po.dest_address_id,
  po.notes AS po_notes,
  po.create_date AS po_created_date,
  po.write_date AS po_updated_date
FROM `{odoo_source}.purchase_order` po
JOIN `{odoo_source}.purchase_order_line` pol ON pol.order_id = po.id
LEFT JOIN `{odoo_source}.res_partner` v ON po.partner_id = v.id
LEFT JOIN `{odoo_source}.account_analytic_account` aaa ON pol.analytic_account_project_id = aaa.id
LEFT JOIN `{odoo_source}.res_users` creator_u ON po.create_uid = creator_u.id
LEFT JOIN `{odoo_source}.res_partner` creator_p ON creator_u.partner_id = creator_p.id
WHERE po.name = 'PO12060'
ORDER BY pol.sequence;
