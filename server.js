const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

const PORT = 3000;
const EPA_API_HOST = 'ghgdata.epa.gov';

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

// Proxy request to EPA API
function proxyToEPA(req, res, apiPath) {
    // Collect request body first
    let bodyChunks = [];

    req.on('data', chunk => bodyChunks.push(chunk));

    req.on('end', () => {
        const body = Buffer.concat(bodyChunks);

        const options = {
            hostname: EPA_API_HOST,
            port: 443,
            path: apiPath,
            method: req.method,
            headers: {
                'Content-Type': req.headers['content-type'] || 'application/json',
                'Accept': req.headers['accept'] || 'application/json',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
        };

        if (body.length > 0) {
            options.headers['Content-Length'] = body.length;
        }

        console.log(`Proxying ${req.method} ${apiPath} to EPA API`);
        if (body.length > 0) {
            console.log(`Request body (${body.length} bytes): ${body.toString().substring(0, 2000)}`);
        }

        const proxyReq = https.request(options, (proxyRes) => {
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
            console.error('Proxy error:', err);
            res.writeHead(500);
            res.end(JSON.stringify({ error: 'Proxy error: ' + err.message }));
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

    // Proxy API requests to EPA
    if (pathname.startsWith('/ghgp/api/')) {
        proxyToEPA(req, res, pathname);
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
