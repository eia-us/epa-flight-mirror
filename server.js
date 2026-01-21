const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

const PORT = 3000;
const LAMBDA_API_HOST = 'localhost';
const LAMBDA_API_PORT = 4000;

// MIME types for serving static files
const MIME_TYPES = {
    '.html': 'text/html',
    '.css': 'text/css',
    '.js': 'application/javascript',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
    '.ttf': 'font/ttf',
    '.eot': 'application/vnd.ms-fontobject'
};

// Proxy request to local Lambda API
function proxyToLambdaAPI(req, res, apiPath) {
    // Collect request body first
    let bodyChunks = [];

    req.on('data', chunk => bodyChunks.push(chunk));

    req.on('end', () => {
        const body = Buffer.concat(bodyChunks);

        const options = {
            hostname: LAMBDA_API_HOST,
            port: LAMBDA_API_PORT,
            path: apiPath,
            method: req.method,
            headers: {
                'Content-Type': req.headers['content-type'] || 'application/json',
                'Accept': req.headers['accept'] || 'application/json'
            }
        };

        if (body.length > 0) {
            options.headers['Content-Length'] = body.length;
        }

        console.log(`Proxying ${req.method} ${apiPath} to Lambda API (localhost:${LAMBDA_API_PORT})`);
        if (body.length > 0) {
            console.log(`Request body (${body.length} bytes): ${body.toString().substring(0, 2000)}`);
        }

        const proxyReq = http.request(options, (proxyRes) => {
            // Collect response
            let responseChunks = [];

            proxyRes.on('data', chunk => responseChunks.push(chunk));

            proxyRes.on('end', () => {
                const responseBody = Buffer.concat(responseChunks);

                console.log(`Response: ${proxyRes.statusCode} (${responseBody.length} bytes)`);
                if (proxyRes.statusCode >= 400 || responseBody.length < 200) {
                    console.log(`Response body: ${responseBody.toString().substring(0, 500)}`);
                }

                // Set CORS headers
                res.setHeader('Access-Control-Allow-Origin', '*');
                res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
                res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Accept');
                res.setHeader('Content-Type', proxyRes.headers['content-type'] || 'application/json');

                res.writeHead(proxyRes.statusCode);
                res.end(responseBody);
            });
        });

        proxyReq.on('error', (err) => {
            console.error('Lambda API proxy error:', err);
            res.writeHead(500);
            res.end(JSON.stringify({ error: 'Lambda API proxy error: ' + err.message + '. Make sure the Lambda API server is running on port ' + LAMBDA_API_PORT }));
        });

        if (body.length > 0) {
            proxyReq.write(body);
        }
        proxyReq.end();
    });
}

// Serve static files
function serveStaticFile(res, filePath) {
    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    fs.readFile(filePath, (err, data) => {
        if (err) {
            if (err.code === 'ENOENT') {
                res.writeHead(404);
                res.end('File not found');
            } else {
                res.writeHead(500);
                res.end('Server error');
            }
            return;
        }
        res.writeHead(200, { 'Content-Type': contentType });
        res.end(data);
    });
}

const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://localhost:${PORT}`);
    let pathname = url.pathname;

    console.log(`${req.method} ${pathname}`);

    // Handle CORS preflight
    if (req.method === 'OPTIONS') {
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
        res.writeHead(204);
        res.end();
        return;
    }

    // Proxy API requests to local Lambda API
    if (pathname.startsWith('/ghgp/api/')) {
        // Include query string in the proxy path
        const fullPath = pathname + url.search;
        proxyToLambdaAPI(req, res, fullPath);
        return;
    }

    // Handle flight routes
    if (pathname.startsWith('/flight')) {
        // Remove /flight prefix for file lookup
        let localPath = pathname.replace(/^\/flight/, '');

        // Default to index.html for root or empty path
        if (localPath === '' || localPath === '/') {
            localPath = '/index.html';
        }

        const filePath = path.join(__dirname, 'flight', localPath);

        // Check if file exists - if not, serve index.html for SPA routing
        // This allows React Router to handle routes like /flight/details/123/2023/GHGRP
        if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
            const indexPath = path.join(__dirname, 'flight', 'index.html');
            serveStaticFile(res, indexPath);
            return;
        }

        serveStaticFile(res, filePath);
        return;
    }

    // Redirect root to /flight/
    if (pathname === '/' || pathname === '') {
        res.writeHead(302, { 'Location': '/flight/?viewType=map' });
        res.end();
        return;
    }

    // 404 for everything else
    res.writeHead(404);
    res.end('Not found');
});

server.listen(PORT, () => {
    console.log(`EPA FLIGHT local server running at http://localhost:${PORT}`);
    console.log(`Open http://localhost:${PORT}/flight/?viewType=map in your browser`);
    console.log('API calls will be proxied to the live EPA server');
});
