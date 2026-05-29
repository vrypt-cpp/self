import mysql from "mysql2/promise";

const requiredEnvVars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"];
for (const envVar of requiredEnvVars) {
  if (!process.env[envVar]) {
    throw new Error(`Environment variable ${envVar} is required but not set.`);
  }
}

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT) || 3306,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: Number(process.env.DB_CONNECTION_LIMIT) || 10,
  queueLimit: 0,
  idleTimeout: 60000,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
  connectTimeout: 10000
});

pool.on("connection", (connection) => {
  console.log(`[DB] New connection established (threadId: ${connection.threadId})`);
  connection.on("error", (err) => {
    console.error(`[DB] Connection error (threadId: ${connection.threadId}):`, err.message);
  });
});

const KEEP_ALIVE_INTERVAL_MS = 30_000;

const keepAliveInterval = setInterval(async () => {
  try {
    const connection = await pool.getConnection();
    await connection.ping();
    connection.release();
    console.log("[DB] Keep-alive ping berhasil");
  } catch (err) {
    console.error("[DB] Keep-alive ping gagal:", (err as Error).message);
  }
}, KEEP_ALIVE_INTERVAL_MS);

keepAliveInterval.unref();

async function closeDatabasePool(): Promise<void> {
  console.log("[DB] Menutup connection pool...");
  clearInterval(keepAliveInterval);
  await pool.end();
  console.log("[DB] Connection pool ditutup.");
}

process.on("SIGINT", async () => {
  await closeDatabasePool();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  await closeDatabasePool();
  process.exit(0);
});

export { pool, closeDatabasePool };
