// Mini http server just to keep container busy
const http = require('http')

const server = http.createServer((req, res) => res.end())
server.listen(8000)
