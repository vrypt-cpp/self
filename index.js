import "dotenv/config";
import makeWASocket, {
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion
} from "@whiskeysockets/baileys";
import pino from "pino";
import http from "node:http";
import { Boom } from "@hapi/boom";
import { exec } from "child_process";
import util from "util";

import { useMySQLAuthState } from "./lib/useMySQLAuthState.js"
import { pool, closeDatabasePool } from "./lib/mysqlDB.js"

const PORT = Number(process.env.PORT) || 3000;
const IS_BUN = typeof Bun !== "undefined";

function startServer() {
  if (IS_BUN) {
    Bun.serve({
      port: PORT,
      fetch(req) {
        return Response.json({ ok: true });
      },
    });
  } else {
    const server = http.createServer((req, res) => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
    });

    server.listen(PORT);
  }

  console.log(`HTTP server running on port ${PORT} (${IS_BUN ? "Bun" : "Node.js"})`);
}

async function startSocket() {
  const { state, saveCreds, clearState } = await useMySQLAuthState(pool, "sessions");
  const { version, isLatest } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    browser: Browsers.macOS("Edge"),
    connectTimeoutMs: 20_000,
    keepAliveIntervalMs: 5_000,
    logger: pino({ level: "silent" }),
    defaultQueryTimeoutMs: 15_000,
    markOnlineOnConnect: false,
    syncFullHistory: false,
  });

  if (!sock.authState.creds.registered) {
    try {
      await new Promise(r => setTimeout(r, 1000));
      const code = await sock.requestPairingCode(process.env.PHONE_NUMBER, "VRYPTBOT");
      console.log("PAIRING CODE:", code);
    } catch (err) {
      console.error("message:", err);
    }
  }

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect } = update;

    if (connection === "close") {
      const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

      console.log("Connection closed, status:", statusCode);

      if (statusCode === DisconnectReason.loggedOut) {
        console.log("Logged out, clearing session...");
        await clearState();
      }

      if (shouldReconnect) {
        setTimeout(() => startSocket(), 3000);
      }
    }

    if (connection === "open") {
      console.log("Connected:", sock.user?.id);
    }
  });

  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify") return;

    for (const msg of messages) {
      if (!msg.message || !msg.key.fromMe) continue;

      const from = msg.key.remoteJid;
      const body =
        msg.message?.conversation ||
        msg.message?.extendedTextMessage?.text ||
        msg.message?.imageMessage?.caption ||
        msg.message?.videoMessage?.caption ||
        "";

      if (!body) continue;

      console.log(`[${from}] ${body}`);

      await handleMessage(sock, msg, from, body);
    }
  });
}

async function handleMessage(sock, msg, from, body) {
  const text = body.trim();
  const lower = text.toLowerCase();

  const sender =
    msg.key.participant ||
    msg.key.remoteJid ||
    "";

  if (lower.startsWith(">")) {
    try {
      const code = text.slice(1).trim();
      const result = await eval(code);
      const output = util.inspect(result, { depth: 10 });

      await sock.sendMessage(from, { text: output }, { quoted: msg });
    } catch (err) {
      await sock.sendMessage(from, { text: String(err) }, { quoted: msg });
    }

    return;
  }

  if (lower.startsWith("$")) {
    const command = text.slice(1).trim();

    exec(command, async (err, stdout, stderr) => {
      if (err) {
        await sock.sendMessage(from, { text: err.message }, { quoted: msg });
        return;
      }

      const output = stdout || stderr || "No output";
      await sock.sendMessage(from, { text: output }, { quoted: msg });
    });

    return;
  }
}

startServer();
startSocket().catch(console.error);
