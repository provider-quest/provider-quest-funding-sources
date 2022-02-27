import fs from 'fs'
import getStream from 'get-stream'
import { S3Client, ListObjectsCommand, GetObjectCommand } from '@aws-sdk/client-s3'

export default async function syncLily (tableName, targetRange) {
  console.log('Sync', tableName, targetRange)
  const workDir = process.env.WORK_DIR || '.'
  const baseDir = `${workDir}/sync/${tableName.replace(/_/g, '-')}`
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
    if (match) {
      return { from: Number(match[1]), to: Number(match[2]) }
    } else {
      return null
    }
  }).filter(record => record).sort(({ from: a }, { from: b }) => a - b)

  // aws s3 ls "s3://lily-data/data/1051440__1054319/power_actor_claims.csv"
  for (const range of ranges) {
    const { from, to } = range
    const target = `${String(from).padStart(10, '0')}__${String(to).padStart(10, '0')}`
    const targetFile = `${workDir}/sync/${tableName.replace(/_/g, '-')}/${target}.csv`
    const targetFileTmp = `${targetFile}.tmp`
    /// console.log('target', target, targetRange, targetFile)
    if (target === targetRange && !fs.existsSync(targetFile)) {
      const key = `data/${from}__${to}/${tableName}.csv`
      console.log('Downloading', key)
      const data = await s3Client.send(new GetObjectCommand({
        Bucket: bucketParams.Bucket,
        Key: key
      }))
      const writeStream = fs.createWriteStream(targetFileTmp)
      data.Body.pipe(writeStream)
      const finished = new Promise((resolve, reject) => {
        data.Body.on('end', resolve)
        writeStream.on('error', reject)
      })
      await finished
      fs.renameSync(targetFileTmp, targetFile)
    }
  }
}
