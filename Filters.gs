/** Filters.gs — interlinked dropdowns from ContractComponents */
registerQuery('filters.getOptions', (p, { bq, cache }) => {
  const sel = normalizeFilter_(p);
  const key = 'filters:' + Utilities.base64EncodeWebSafe(JSON.stringify(sel));

  return cache(key, 300, () => {
    const T = `\`${BQ_PROJECT_ID}.${BQ_DATASET}.ContractComponents\``;
    return {
      customers:        distinct_(bq, T, 'CustomerName',   sel, 'customerName'),
      commercialNames:  distinct_(bq, T, 'ClientName',     sel, 'clientName'),
      accountManagers:  distinct_(bq, T, 'AccountManager', sel, 'accountManager'),
      statuses:         distinct_(bq, T, 'Status',         sel, 'status'),
      selected: sel
    };
  });
});

/* ---------- helpers ---------- */
function normalizeFilter_(p) {
  p = p || {};
  return {
    customerName:   toNull_(p.customerName),
    clientName:     toNull_(p.clientName),
    accountManager: toNull_(p.accountManager),
    status:         toNull_(p.status)
  };
}

function distinct_(bq, table, col, sel, excludeKey) {
  const w = buildWhere_(sel, excludeKey);
  const rows = bq.run(
    `SELECT DISTINCT ${col} AS v FROM ${table} ${w.where} ORDER BY v`,
    w.params
  );
  return rows.filter(r => r.v !== null && r.v !== '').map(r => r.v);
}

function buildWhere_(sel, excludeKey) {
  const cond = [], params = {};
  if (sel.customerName   && excludeKey !== 'customerName')   { cond.push('CustomerName = @customerName');     params.customerName   = sel.customerName; }
  if (sel.clientName     && excludeKey !== 'clientName')     { cond.push('ClientName = @clientName');         params.clientName     = sel.clientName; }
  if (sel.accountManager && excludeKey !== 'accountManager') { cond.push('AccountManager = @accountManager'); params.accountManager = sel.accountManager; }
  if (sel.status         && excludeKey !== 'status')         { cond.push('Status = @status');                 params.status         = sel.status; }
  return { where: cond.length ? ('WHERE ' + cond.join(' AND ')) : '', params };
}

function toNull_(v) {
  return (v === undefined || v === null || v === '') ? null : String(v);
}
