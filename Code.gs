/*********************************************************
 * Account Management Dashboard — Core (Code.gs)
 * Project: masterdata-470911 / Dataset: master_data
 * Requires: Advanced Service "BigQuery" enabled in Apps Script
 **********************************************************/

/** ========= BigQuery CONFIG ========= **/
const BQ_PROJECT_ID = 'masterdata-470911';
const BQ_DATASET    = 'master_data';
const BQ_LOCATION   = 'US'; // e.g. 'US' | 'EU' | 'asia-south2' ; leave '' to let default apply

/** ========= UI ENTRY ========= **/
function doGet() {
  return HtmlService.createHtmlOutputFromFile('Dashboard')
    .setTitle('Account Management Dashboard')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/** Minimal config for the client (optional) */
function getProjectConfig() {
  return { projectId: BQ_PROJECT_ID, dataset: BQ_DATASET, location: BQ_LOCATION || null };
}

/** Router: call a server-side query by name (registered from other .gs files) */
function execQuery(name, payload) {
  if (!QUERY_REGISTRY[name]) throw new Error('Unknown query: ' + name);
  // add helpers into context:
  const ctx = { bq: BQ, select: select_, cache: withCache_ };
  return QUERY_REGISTRY[name](payload || {}, ctx);
}

/** (Optional) Direct SELECT for simple tables; safe & parameterized */
function select_(table, opts) {
  const o = Object.assign({ columns: '*', where: '', orderBy: '', limit: null, params: {} }, opts || {});
  const cols = Array.isArray(o.columns) ? o.columns.join(', ') : o.columns;
  const sql =
    `SELECT ${cols}
       FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${table}\`
      ${o.where ? 'WHERE ' + o.where : ''}
      ${o.orderBy ? 'ORDER BY ' + o.orderBy : ''}
      ${o.limit ? 'LIMIT ' + Number(o.limit) : ''}`;
  return BQ.run(sql, o.params);
}

/** ========= REUSABLE BigQuery CLIENT ========= **/
const BQ = (() => {
  return {
    run, runPaged, param, paramsFrom, listTables, getTableSchema, tableExists
  };

  /** Run a query and return all rows (typed) */
  function run(sql, params = {}, options = {}) {
    if (!/^select\s/i.test(sql.trim())) {
      throw new Error('Only SELECT queries are permitted via BQ.run()');
    }
    const cfg = {
      configuration: {
        query: {
          query: sql,
          useLegacySql: false,
          parameterMode: 'NAMED',
          queryParameters: buildParams_(params)
        }
      }
    };
    if (BQ_LOCATION) cfg.location = BQ_LOCATION;

    const job = BigQuery.Jobs.insert(cfg, BQ_PROJECT_ID);
    const jobId = job.jobReference.jobId;

    let rows = [];
    let pageToken = null;
    do {
      const res = BigQuery.Jobs.getQueryResults(
        BQ_PROJECT_ID,
        jobId,
        Object.assign({}, (BQ_LOCATION ? { location: BQ_LOCATION } : {}), pageToken ? { pageToken } : {})
      );
      const chunk = mapRows_(res, options);
      rows = rows.concat(chunk);
      pageToken = res.pageToken || null;
    } while (pageToken);

    return rows;
  }

  /** For very large results, page manually (returns {rows,nextPageToken}) */
  function runPaged(sql, params = {}, options = {}, pageToken) {
    const cfg = {
      configuration: {
        query: {
          query: sql,
          useLegacySql: false,
          parameterMode: 'NAMED',
          queryParameters: buildParams_(params)
        }
      }
    };
    if (BQ_LOCATION) cfg.location = BQ_LOCATION;

    const job = BigQuery.Jobs.insert(cfg, BQ_PROJECT_ID);
    const jobId = job.jobReference.jobId;

    const res = BigQuery.Jobs.getQueryResults(
      BQ_PROJECT_ID,
      jobId,
      Object.assign({}, (BQ_LOCATION ? { location: BQ_LOCATION } : {}), pageToken ? { pageToken } : {})
    );
    return { rows: mapRows_(res, options), nextPageToken: res.pageToken || null };
  }

  /** Build one parameter (use in array form when you need explicit typing) */
  function param(name, value, type /* 'STRING'|'INT64'|'FLOAT64'|'BOOL'|'DATE'|'TIMESTAMP' */) {
    return { _kind: 'bqParam', name, value, type: type || inferType_(value) };
  }

  /** Convert object or array into BigQuery named parameters */
  function paramsFrom(objOrArr) {
    return buildParams_(objOrArr);
  }

  /** Introspection helpers */
  function listTables() {
    const out = BigQuery.Tables.list(BQ_PROJECT_ID, BQ_DATASET);
    return (out.tables || []).map(t => t.tableReference.tableId);
  }
  function getTableSchema(table) {
    const t = BigQuery.Tables.get(BQ_PROJECT_ID, BQ_DATASET, table);
    return t.schema || { fields: [] };
  }
  function tableExists(table) {
    try { BigQuery.Tables.get(BQ_PROJECT_ID, BQ_DATASET, table); return true; }
    catch (e) { return false; }
  }

  /* ======== private helpers ======== */

  function buildParams_(objOrArr) {
    if (Array.isArray(objOrArr)) {
      // expecting array of BQ.param()
      return objOrArr.map(p => toBQParam_(p.name, p.value, p.type || inferType_(p.value)));
    }
    // object -> infer types
    const out = [];
    Object.keys(objOrArr || {}).forEach(k => {
      const v = objOrArr[k];
      out.push(toBQParam_(k, v, inferType_(v)));
    });
    return out;
  }

  function toBQParam_(name, value, type) {
    // arrays (STRING/INT64/FLOAT64/BOOL only in this helper)
    if (Array.isArray(value)) {
      const arrayType = type || inferType_(value[0] ?? '');
      return {
        name,
        parameterType: { type: 'ARRAY', arrayType: { type: arrayType } },
        parameterValue: { arrayValues: value.map(v => ({ value: v === null || v === undefined ? null : String(v) })) }
      };
    }
    // scalars
    return {
      name,
      parameterType: { type: type },
      parameterValue: { value: value === null || value === undefined ? null : String(value) }
    };
  }

  function inferType_(v) {
    if (v === null || v === undefined) return 'STRING';
    if (Array.isArray(v)) return 'STRING'; // actual array typing handled in toBQParam_
    if (typeof v === 'number') return Number.isInteger(v) ? 'INT64' : 'FLOAT64';
    if (typeof v === 'boolean') return 'BOOL';
    if (isIsoDate_(v)) return 'DATE';
    if (isIsoDateTime_(v)) return 'TIMESTAMP';
    return 'STRING';
  }

  function isIsoDate_(s) {
    return typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s);
  }
  function isIsoDateTime_(s) {
    return typeof s === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(s);
  }

  function mapRows_(res, options) {
    const fields = (res.schema && res.schema.fields) ? res.schema.fields : [];
    const coerce = options && options.coerce !== false;  // default true
    const dateAs = (options && options.dateAs) || 'string'; // 'string' | 'Date'
    const rows = res.rows || [];
    return rows.map(r => {
      const obj = {};
      r.f.forEach((cell, i) => {
        const f = fields[i] || { name: 'col' + i, type: 'STRING' };
        obj[f.name] = coerce ? coerceValue_(cell.v, f.type, dateAs) : cell.v;
      });
      return obj;
    });
  }

  function coerceValue_(v, type, dateAs) {
    if (v === null || v === undefined) return null;
    switch (type) {
      case 'INT64':
      case 'INTEGER':
      case 'FLOAT64':
      case 'FLOAT':
      case 'NUMERIC':
      case 'BIGNUMERIC':
        return Number(v);
      case 'BOOL':
      case 'BOOLEAN':
        return v === true || v === 'true';
      case 'DATE':
        return dateAs === 'Date' ? new Date(v + 'T00:00:00Z') : String(v);
      case 'TIMESTAMP':
      case 'DATETIME':
        return dateAs === 'Date' ? new Date(v) : String(v);
      default:
        return String(v);
    }
  }
})();

/** ========= LIGHTWEIGHT CACHE ========= **/
function withCache_(key, ttlSeconds, producer) {
  const cache = CacheService.getScriptCache();
  const hit = cache.get(key);
  if (hit) return JSON.parse(hit);
  const fresh = producer();
  cache.put(key, JSON.stringify(fresh), Math.max(1, Math.min(21600, ttlSeconds || 60))); // cap 6h
  return fresh;
}

/** ========= SIMPLE REGISTRY FOR MODULAR QUERIES ========= **/
const QUERY_REGISTRY = Object.create(null);
/**
 * Other .gs files can call registerQuery('name', (payload, {bq,select,cache}) => {...return data...})
 */
function registerQuery(name, fn) {
  if (typeof fn !== 'function') throw new Error('registerQuery requires a function');
  QUERY_REGISTRY[name] = fn;
}
