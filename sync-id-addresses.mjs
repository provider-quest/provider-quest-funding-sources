import fs from 'fs'
import getStream from 'get-stream'
import { S3Client, ListObjectsCommand, GetObjectCommand } from '@aws-sdk/client-s3'

try {
  const baseDir = './sync/id-addresses'
  await fs.mkdirSync(baseDir, { recursive: true })
  const s3Client = new S3Client({
    region: 'us-east-2'
  })
  const bucketParams = {
    Bucket: 'lily-data',
    Prefix: 'data/',
    Delimiter: '/'
  }
  const data = await s3Client.send(new ListObjectsCommand(bucketParams))
  const ranges = data.CommonPrefixes.map(({ Prefix: prefix }) => {
    const match = prefix.match(/^data\/(\d+)_+(\d+)\/$/)
    return { from: Number(match[1]), to: Number(match[2]) }
  }).sort(({ from: a }, { from: b }) => a - b)

  // aws s3 ls "s3://lily-data/data/1051440__1054319/power_actor_claims.csv"
  for (const range of ranges) {
    const { from, to } = range
    if (to > 300000) break
    const target = `${baseDir}/${String(from).padStart(10, '0')}__${String(to).padStart(10, '0')}.csv`
    if (!fs.existsSync(target)) {
      const key = `data/${from}__${to}/id_addresses.csv`
      console.log(range, key)
      const data = await s3Client.send(new GetObjectCommand({
        Bucket: bucketParams.Bucket,
        Key: key
      }))
      let contents = await getStream(data.Body)
      fs.writeFileSync(target, contents)
    }
  }
} catch (err) {
  console.error("Error", err)
}
