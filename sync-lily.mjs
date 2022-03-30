import fs from 'fs'
import getStream from 'get-stream'
import { S3Client, ListObjectsCommand, GetObjectCommand } from '@aws-sdk/client-s3'
import { pipeline } from 'stream/promises'
import zlib from 'zlib'

export default async function syncLily (tableName, date) {
  console.log('Sync', tableName, date)
  const workDir = process.env.WORK_DIR || '.'
  const baseDir = `${workDir}/sync/${tableName.replace(/_/g, '-')}`
  await fs.mkdirSync(baseDir, { recursive: true })
  const s3Client = new S3Client({
    region: 'us-east-2'
  })
  const dirPrefix = `mainnet/csv/1/${tableName}/${date.slice(0,4)}/`
  const bucketParams = {
    Bucket: 'fil-archive',
    Prefix: 'data/',
    Delimiter: '/'
  }

  // aws s3 ls s3://fil-archive/mainnet/csv/1/power_actor_claims/2022/power_actor_claims-2022-03-27.csv.gz
  const targetFile = `${workDir}/sync/${tableName.replace(/_/g, '-')}/${date}.csv`
  const targetFileTmp = `${targetFile}.tmp`
  /// console.log('target', target, date, targetFile)
  if (!fs.existsSync(targetFile)) {
    const key = `${dirPrefix}${tableName}-${date}.csv.gz`
    console.log('Downloading', key)
    const data = await s3Client.send(new GetObjectCommand({
      Bucket: bucketParams.Bucket,
      Key: key
    }))
    await pipeline(
      data.Body,
      zlib.createGunzip(),
      fs.createWriteStream(targetFileTmp)
    )
    fs.renameSync(targetFileTmp, targetFile)
  }
}
