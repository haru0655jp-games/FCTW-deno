type RoomType = "public" | "private" | "hidden";

type Room = {
  id: string;
  name: string;
  type: RoomType;
  code: string; // publicは空
  url: string;
  ownerName: string;
  createdAt: string;
};

type ChatUser = {
  id: string;
  userName: string;
  createdAt: string;
};

type ChatMessage = {
  id: string;
  roomId: string;
  userName: string;
  text: string;
  createdAt: string;
};

const kv = await Deno.openKv();
const socketsByRoom = new Map<string, Set<WebSocket>>();

const corsHeaders = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET,POST,OPTIONS",
  "access-control-allow-headers": "content-type",
};

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      ...corsHeaders,
      "content-type": "application/json; charset=utf-8",
    },
  });
}

function text(data: string, status = 200) {
  return new Response(data, { status, headers: corsHeaders });
}

function randomId() {
  return crypto.randomUUID();
}

function randomCode(length = 6) {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let out = "";
  for (let i = 0; i < length; i++) {
    out += chars[Math.floor(Math.random() * chars.length)];
  }
  return out;
}

function normalizeType(value: unknown): RoomType {
  const t = String(value ?? "public").toLowerCase();
  if (t === "private" || t === "hidden") return t;
  return "public";
}

async function getRoomById(roomId: string): Promise<Room | null> {
  const result = await kv.get<Room>(["room", roomId]);
  return result.value ?? null;
}

async function getRoomByCode(code: string): Promise<Room | null> {
  const index = await kv.get<string>(["roomCode", code.toUpperCase()]);
  if (!index.value) return null;
  return await getRoomById(index.value);
}

async function saveRoom(room: Room) {
  await kv.set(["room", room.id], room);
  if (room.code) {
    await kv.set(["roomCode", room.code], room.id);
  }
}

async function saveUser(user: ChatUser) {
  await kv.set(["user", user.id], user);
}

async function saveMessage(message: ChatMessage) {
  await kv.set(["message", message.roomId, message.createdAt, message.id], message);
}

async function listMessages(roomId: string, limit = 50): Promise<ChatMessage[]> {
  const items: ChatMessage[] = [];
  for await (const entry of kv.list<ChatMessage>({ prefix: ["message", roomId] })) {
    items.push(entry.value);
  }
  return items.slice(-limit);
}

function broadcast(roomId: string, payload: unknown) {
  const set = socketsByRoom.get(roomId);
  if (!set) return;

  const data = JSON.stringify(payload);
  for (const socket of [...set]) {
    try {
      socket.send(data);
    } catch {
      set.delete(socket);
    }
  }

  if (set.size === 0) socketsByRoom.delete(roomId);
}

async function parseJson(req: Request) {
  try {
    return await req.json();
  } catch {
    return null;
  }
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return text("ok", 204);

  const url = new URL(req.url);
  const parts = url.pathname.split("/").filter(Boolean);

  if (req.method === "GET" && url.pathname === "/health") {
    return json({ ok: true });
  }

  if (req.method === "GET" && parts[0] === "api" && parts[1] === "rooms" && parts[2]) {
    const room = await getRoomById(parts[2]);
    if (!room) return json({ error: "room not found" }, 404);
    return json({ room });
  }

  if (req.method === "GET" && parts[0] === "api" && parts[1] === "rooms" && parts[2] === "messages") {
    const roomId = parts[2];
    const limit = Math.max(1, Math.min(200, Number(url.searchParams.get("limit") ?? "50")));
    const room = await getRoomById(roomId);
    if (!room) return json({ error: "room not found" }, 404);

    const messages = await listMessages(roomId, limit);
    return json({ roomId, messages });
  }

  if (req.method === "GET" && url.pathname === "/ws") {
    const roomId = url.searchParams.get("roomId") ?? "";
    const userName = url.searchParams.get("userName") ?? "guest";
    const room = roomId ? await getRoomById(roomId) : null;

    if (!room) return json({ error: "room not found" }, 404);

    const upgrade = req.headers.get("upgrade")?.toLowerCase();
    if (upgrade !== "websocket") {
      return json({ error: "websocket required" }, 400);
    }

    const { socket, response } = Deno.upgradeWebSocket(req);

    let set = socketsByRoom.get(roomId);
    if (!set) {
      set = new Set<WebSocket>();
      socketsByRoom.set(roomId, set);
    }
    set.add(socket);

    socket.onopen = () => {
      socket.send(JSON.stringify({
        type: "ready",
        roomId,
        userName,
      }));
    };

    socket.onmessage = (event) => {
      try {
        const data = JSON.parse(String(event.data));
        if (data?.type === "ping") {
          socket.send(JSON.stringify({ type: "pong", at: new Date().toISOString() }));
        }
      } catch {
        // ignore
      }
    };

    socket.onclose = () => {
      const current = socketsByRoom.get(roomId);
      current?.delete(socket);
      if (current && current.size === 0) socketsByRoom.delete(roomId);
    };

    socket.onerror = () => {
      const current = socketsByRoom.get(roomId);
      current?.delete(socket);
      if (current && current.size === 0) socketsByRoom.delete(roomId);
    };

    return response;
  }

  if (req.method === "POST" && url.pathname === "/api/users") {
    const body = await parseJson(req);
    if (!body) return json({ error: "invalid json" }, 400);

    const userName = String(body.userName ?? "").trim();
    if (!userName) return json({ error: "userName required" }, 400);

    const user: ChatUser = {
      id: randomId(),
      userName,
      createdAt: new Date().toISOString(),
    };

    await saveUser(user);
    return json({ user });
  }

  if (req.method === "POST" && url.pathname === "/api/rooms") {
    const body = await parseJson(req);
    if (!body) return json({ error: "invalid json" }, 400);

    const name = String(body.name ?? "").trim();
    const ownerName = String(body.ownerName ?? "owner").trim();
    const type = normalizeType(body.type);

    if (!name) return json({ error: "name required" }, 400);

    const id = randomId();
    const code = type === "public" ? "" : randomCode(6);
    const room: Room = {
      id,
      name,
      type,
      code,
      url: new URL(`/room/${id}`, url.origin).toString(),
      ownerName,
      createdAt: new Date().toISOString(),
    };

    await saveRoom(room);
    return json({ room });
  }

  if (req.method === "POST" && url.pathname === "/api/rooms/join") {
    const body = await parseJson(req);
    if (!body) return json({ error: "invalid json" }, 400);

    const roomId = String(body.roomId ?? "").trim();
    const code = String(body.code ?? "").trim().toUpperCase();

    let room: Room | null = null;

    if (roomId) room = await getRoomById(roomId);
    if (!room && code) room = await getRoomByCode(code);

    if (!room) return json({ error: "room not found" }, 404);

    return json({ room });
  }

  if (req.method === "POST" && url.pathname === "/api/messages") {
    const body = await parseJson(req);
    if (!body) return json({ error: "invalid json" }, 400);

    const roomId = String(body.roomId ?? "").trim();
    const userName = String(body.userName ?? "guest").trim();
    const textValue = String(body.text ?? "").trim();

    if (!roomId) return json({ error: "roomId required" }, 400);
    if (!textValue) return json({ error: "text required" }, 400);

    const room = await getRoomById(roomId);
    if (!room) return json({ error: "room not found" }, 404);

    const message: ChatMessage = {
      id: randomId(),
      roomId,
      userName,
      text: textValue,
      createdAt: new Date().toISOString(),
    };

    await saveMessage(message);
    broadcast(roomId, { type: "message", message });

    return json({ ok: true, message });
  }

  if (req.method === "GET" && url.pathname === "/api/messages") {
    const roomId = String(url.searchParams.get("roomId") ?? "").trim();
    const limit = Math.max(1, Math.min(200, Number(url.searchParams.get("limit") ?? "50")));

    if (!roomId) return json({ error: "roomId required" }, 400);

    const room = await getRoomById(roomId);
    if (!room) return json({ error: "room not found" }, 404);

    const messages = await listMessages(roomId, limit);
    return json({ roomId, messages });
  }

  return json({ error: "not found" }, 404);
});
