const http = require('http')

const options = {
  method: 'POST',
  hostname: 'localhost',
  port: 9000,
  path: '/price',
  headers: {
    'content-type': 'application/json'
  }
}

const req = http.request(options, res => {
  const chunks = []

  res.on('data', chunk => {
    chunks.push(chunk)
  })

  res.on('end', () => {
    const body = Buffer.concat(chunks)
    console.log(body.toString())
  })
})

req.write(JSON.stringify({
    instrument: 'MSFT',
    granularity: 'D',
    start: '2021-11-01T00:00:00',
    end: '2021-12-01T00:00:00'
  }))
req.end()
