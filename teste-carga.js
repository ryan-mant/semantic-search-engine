import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';

const ingestDuration = new Trend('ingest_duration');
const searchDuration = new Trend('search_duration');

export const options = {
    stages: [
        { duration: '5s', target: 50 },  // Ramp-up
        { duration: '20s', target: 100 }, // Load
        { duration: '5s', target: 0 },   // Ramp-down
    ],
    thresholds: {
        http_req_failed: ['rate<0.01'], // less than 1% errors
    },
};

const documents = [
    "The company policy requires all employees to submit their monthly expense reports by the last Friday of each month.",
    "Our semantic search engine uses sentence-transformers to generate dense vectors for text matching.",
    "To connect to PostgreSQL from Python, we can use asyncpg or psycopg2 adapters.",
    "Kubernetes orchestration allows scaling worker pods horizontally based on CPU utilization metrics.",
    "Apache Kafka writes all incoming messages to an append-only log on disk for high-throughput persistence.",
    "Docker containers simplify deployment by packaging code and dependencies together.",
    "FastAPI is a modern web framework for building APIs with Python 3.8+ based on standard Python type hints.",
    "SentenceTransformers sentence embeddings are generated using pretrained models for semantic text similarity."
];

const queries = [
    "expense report deadline",
    "dense vector embeddings",
    "postgresql python connection",
    "horizontal scaling pods",
    "kafka write path",
    "docker deployment benefits",
    "fastapi python framework",
    "semantic text similarity"
];

export default function () {
    const url = 'http://localhost:8000';
    
    // Choose randomly between Ingestion (60%) and Search (40%)
    if (Math.random() < 0.6) {
        // --- INGEST DOCUMENT ---
        const payload = JSON.stringify({
            content: documents[Math.floor(Math.random() * documents.length)] + " Random hash: " + Math.random().toString(36).substring(7),
            metadata: {
                user_id: String(Math.floor(Math.random() * 1000)),
                category: "loadtest",
                author: "k6-runner"
            }
        });
        
        const params = {
            headers: {
                'Content-Type': 'application/json',
            },
        };
        
        const res = http.post(`${url}/documents/ingest`, payload, params);
        ingestDuration.add(res.timings.duration);
        
        check(res, {
            'ingest status is 201': (r) => r.status === 201,
            'ingest response has id': (r) => JSON.parse(r.body).id !== undefined,
        });
    } else {
        // --- SEMANTIC SEARCH ---
        const query = queries[Math.floor(Math.random() * queries.length)];
        const res = http.get(`${url}/documents/search?q=${encodeURIComponent(query)}`);
        searchDuration.add(res.timings.duration);
        
        check(res, {
            'search status is 200': (r) => r.status === 200,
            'search returned list': (r) => Array.isArray(JSON.parse(r.body)),
        });
    }
    
    sleep(0.01); // 10ms pacing to avoid CPU starvation on the host
}
