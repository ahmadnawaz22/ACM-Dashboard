/** Health.gs — Client Health
 *  Match key: CustomerName (filter) == Clients.CompanyName
 *  Source: masterdata-470911.master_data.Clients
 */
registerQuery('client.health', (p, { bq }) => {
  if (!p || !p.customerName) return {};

  const rows = bq.run(
    `SELECT
       ClientSince,
       LifeTimemonths,
       RenewalDate,
       MonthstoRenewal,
       LicenseType,
       RenewalType,
       Rollover,
       Status
     FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.Clients\`
     WHERE CompanyName = @company
     LIMIT 1`,
    { company: p.customerName }
  );

  const r = rows[0] || {};
  return {
    client_since: r.ClientSince || null,
    lifetime_months: r.LifeTimemonths || null,     // note the column spelling you provided
    renewal_date: r.RenewalDate || null,
    months_to_renewal: r.MonthstoRenewal || null,
    license_type: r.LicenseType || null,
    renewal_type: r.RenewalType || null,
    rollover: r.Rollover,                          // may be BOOL or string
    status: r.Status || null
  };
});
