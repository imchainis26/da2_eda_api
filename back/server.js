import express from 'express';
import { createServer } from 'http';
import cors from 'cors';
import pg from 'pg';
const { Pool } = pg;
import { Server as SocketIO } from "socket.io";


const app = express();
const httpServer = createServer(app);
const io = new SocketIO(httpServer, {
  cors: { origin: "*" }
});

app.use(cors());
app.use(express.json());

const pool = new Pool({
    host: process.env.DB_HOST || '172.31.19.186',
    port: process.env.DB_PORT || 5432,
    database: process.env.DB_NAME || 'citypass_logs',
    user: process.env.DB_USER || 'citypass',
    password: process.env.DB_PASSWORD || 'citypass',
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
});

pool.on('connect', () => {
    console.log('Conectado a PostgreSQL');
});

pool.on('error', (err) => {
    console.error('Error en PostgreSQL:', err);
});

app.get("/logs/count", async (req, res) => {
  const result = await pool.query("SELECT COUNT(*) FROM logs");
  res.json({ total: parseInt(result.rows[0].count, 10) });
});

app.get('/messages', async (req, res) => {
    try {        
        const result = await pool.query(
            'SELECT * FROM logs ORDER BY id DESC LIMIT 100;',
        );
        res.json(result.rows);
    } catch (error) {
        console.error('Error obteniendo mensajes:', error);
        res.status(500).json({ error: 'Error obteniendo mensajes' });
    }
});

app.get('/search', async (req, res) => {
    try {
        const {
            id,
            user,
            app_id,
            state,
            routing_keys,
            publisher,
            subscriber,
            exchange_name,
            node,
            date_from,
            date_to
        } = req.query;

        console.log('📋 Parámetros de búsqueda recibidos:', req.query);

        let query = 'SELECT * FROM logs WHERE 1=1';
        const values = [];
        let paramCount = 1;

        if (id) {
            query += ` AND CAST(id AS TEXT) = $${paramCount}`;
            values.push(id);
            paramCount++;
        }

        if (user) {
            query += ` AND CAST("user" AS TEXT) ILIKE $${paramCount}`;
            values.push(`%${user}%`);
            paramCount++;
        }

        if (app_id) {
            query += ` AND CAST(app_id AS TEXT) ILIKE $${paramCount}`;
            values.push(`%${app_id}%`);
            paramCount++;
        }

        if (state) {
            query += ` AND CAST(state AS TEXT) ILIKE $${paramCount}`;
            values.push(`%${state}%`);
            paramCount++;
        }

        if (routing_keys) {
            query += ` AND CAST(routing_keys AS TEXT) ILIKE $${paramCount}`;
            values.push(`%${routing_keys}%`);
            paramCount++;
        }

        if (publisher) {
            query += ` AND CAST(publisher AS TEXT) ILIKE $${paramCount}`;
            values.push(`%${publisher}%`);
            paramCount++;
        }

        if (subscriber) {
            query += ` AND CAST(subscriber AS TEXT) ILIKE $${paramCount}`;
            values.push(`%${subscriber}%`);
            paramCount++;
        }

        if (exchange_name) {
            query += ` AND CAST(exchange_name AS TEXT) ILIKE $${paramCount}`;
            values.push(`%${exchange_name}%`);
            paramCount++;
        }

        if (node) {
            query += ` AND CAST(node AS TEXT) ILIKE $${paramCount}`;
            values.push(`%${node}%`);
            paramCount++;
        }

        if (date_from) {
            query += ` AND event_ts >= $${paramCount}::date`;
            values.push(date_from);
            paramCount++;
        }

        if (date_to) {
            query += ` AND event_ts < ($${paramCount}::date + interval '1 day')`;
            values.push(date_to);
            paramCount++;
}

        query += ' ORDER BY event_ts DESC LIMIT 500';

        console.log('🔍 Query construida:', query);
        console.log('📊 Valores:', values);

        const result = await pool.query(query, values);
        
        console.log(`✅ Resultados encontrados: ${result.rows.length}`);
        
        res.json(result.rows);
    } catch (error) {
        console.error('❌ Error en búsqueda:', error.message);
        console.error('Stack:', error.stack);
        res.status(500).json({ 
            error: 'Error al realizar la búsqueda',
            details: error.message 
        });
    }
});

io.on('connection', (socket) => {
    console.log('Cliente conectado:', socket.id);
    
    socket.emit('connection_status', { 
        connected: true, 
        timestamp: new Date() 
    });
    
    socket.on('health_check', () => {
        socket.emit('health_response', { 
            status: 'ok', 
            timestamp: new Date() 
        });
    });
    
    socket.on('disconnect', () => {
        console.log('Cliente desconectado:', socket.id);
    });
});

const PORT = 5000;
httpServer.listen(PORT, () => {
    console.log(`Servidor EDA Monitor ejecutándose`);
});