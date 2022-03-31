import { S3Client, ListObjectsCommand } from "@aws-sdk/client-s3"
import lilyDates from './lily-dates.mjs'

try {
  const dates = await lilyDates('miner_infos')
  for (const date of dates) {
    console.log(date)
  }
} catch (err) {
  console.error("Error", err)
}
