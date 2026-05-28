import { initAuthCreds, BufferJSON, proto } from '@whiskeysockets/baileys'

function assertSafeName(name, label = 'name') {
  if (typeof name !== 'string' || !/^[a-zA-Z0-9_]+$/.test(name)) {
    throw new Error(`[useMySQLAuthState] Unsafe ${label}: "${name}". Only alphanumeric and underscores are allowed.`)
  }
}

const serialize   = (val) => JSON.stringify(val, BufferJSON.replacer)
const deserialize = (raw) => {
  try { return JSON.parse(raw, BufferJSON.reviver) } catch { return null }
}

const poolTableInitMap = new WeakMap()

const DDL = (table) => `
  CREATE TABLE IF NOT EXISTS \`${table}\` (
    \`session\`    VARCHAR(128) NOT NULL,
    \`id\`         VARCHAR(255) NOT NULL,
    \`data\`       MEDIUMTEXT   NOT NULL,
    \`updated_at\` TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (\`session\`, \`id\`),
    INDEX \`idx_session\` (\`session\`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`

async function ensureTable(pool, table) {
  if (!poolTableInitMap.has(pool)) {
    poolTableInitMap.set(pool, new Map())
  }
  const tableMap = poolTableInitMap.get(pool)

  if (!tableMap.has(table)) {
    const p = pool.query(DDL(table)).catch(err => {
      tableMap.delete(table)
      throw err
    })
    tableMap.set(table, p)
  }

  await tableMap.get(table)
}

async function withRetry(fn, retries = 3, delayMs = 200) {
  let lastErr
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fn()
    } catch (err) {
      lastErr = err
      const isTransient = err.code && [
        'ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT',
        'ER_LOCK_DEADLOCK', 'ER_LOCK_WAIT_TIMEOUT',
      ].includes(err.code)

      if (!isTransient || attempt === retries) break

      await new Promise(res => setTimeout(res, delayMs * 2 ** attempt))
    }
  }
  throw lastErr
}

export async function useMySQLAuthState(pool, session, table = 'baileys_auth') {
  assertSafeName(table,   'table')
  assertSafeName(session, 'session')

  await ensureTable(pool, table)

  const read = async (id) => {
    const [[row]] = await pool.query(
      `SELECT data FROM \`${table}\` WHERE session = ? AND id = ? LIMIT 1`,
      [session, id]
    )
    return row ? deserialize(row.data) : null
  }

  const readMany = async (ids) => {
    if (!ids.length) return {}

    let rows
    if (ids.length === 1) {
      ;[rows] = await pool.query(
        `SELECT id, data FROM \`${table}\` WHERE session = ? AND id = ?`,
        [session, ids[0]]
      )
    } else {
      ;[rows] = await pool.query(
        `SELECT id, data FROM \`${table}\` WHERE session = ? AND id IN (?)`,
        [session, ids]
      )
    }

    return Object.fromEntries(rows.map(r => [r.id, deserialize(r.data)]))
  }

  const writeMany = async (entries) => {
    if (!entries.length) return
    const values = entries.map(([id, val]) => [session, id, val])
    await pool.query(
      `INSERT INTO \`${table}\` (session, id, data) VALUES ?
       ON DUPLICATE KEY UPDATE data = VALUES(data), updated_at = NOW()`,
      [values]
    )
  }

  const removeMany = async (ids) => {
    if (!ids.length) return
    await pool.query(
      `DELETE FROM \`${table}\` WHERE session = ? AND id IN (?)`,
      [session, ids]
    )
  }

  const creds = (await read('creds')) ?? initAuthCreds()

  const keys = {
    async get(type, ids) {
      const result = await withRetry(() => readMany(ids.map(id => `${type}-${id}`)))
      return Object.fromEntries(
        ids.map(id => {
          let val = result[`${type}-${id}`] ?? null
          if (val != null && type === 'app-state-sync-key') {
            val = proto.Message.AppStateSyncKeyData.fromObject(val)
          }
          return [id, val]
        })
      )
    },

    async set(data) {
      const toWrite  = []
      const toDelete = []
      for (const [type, entries] of Object.entries(data)) {
        for (const [id, val] of Object.entries(entries)) {
          const key = `${type}-${id}`
          if (val != null) toWrite.push([key, serialize(val)])
          else             toDelete.push(key)
        }
      }
      await Promise.all([
        withRetry(() => writeMany(toWrite)),
        withRetry(() => removeMany(toDelete)),
      ])
    },
  }

  const state = { creds, keys }

  const saveCreds = () => withRetry(() => writeMany([['creds', serialize(state.creds)]]))

  const clearState = async () => {
    await pool.query(
      `DELETE FROM \`${table}\` WHERE session = ?`,
      [session]
    )
  }

  return { state, saveCreds, clearState }
}
