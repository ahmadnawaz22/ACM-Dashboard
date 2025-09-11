/** ClientInfo.gs — Key Account Information
 *  Match key: CustomerName (filter) == Clients.CompanyName
 *  Source: masterdata-470911.master_data.Clients
 */
registerQuery('client.info', (p, { bq }) => {
  if (!p || !p.customerName) return {};  // nothing selected yet

  const rows = bq.run(
    `SELECT
       CommercialName,
       DealType,
       AccountManager,
       CustomerSuccess,
       ExecutiveSponsor,
       SupportType,
       DealCategory,
       ActiveCountries
     FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.Clients\`
     WHERE CompanyName = @company
     LIMIT 1`,
    { company: p.customerName }
  );

  const r = rows[0] || {};
  return {
    customer_name: p.customerName,                         // keep same as selected
    commercial_name: r.CommercialName || null,
    deal_type: r.DealType || null,
    account_manager: r.AccountManager || null,
    customer_success: r.CustomerSuccess || null,
    executive_sponsor: r.ExecutiveSponsor || null,
    support_type: r.SupportType || null,
    deal_category: r.DealCategory || null,
    active_countries: r.ActiveCountries || null           // string or array; HTML handles both
  };
});
