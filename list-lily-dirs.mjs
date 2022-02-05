import { S3Client, ListObjectsCommand } from "@aws-sdk/client-s3"
import lilyRanges from './lily-ranges.mjs'

try {
  const ranges = await lilyRanges()
  for (const range of ranges) {
    console.log(range)
  }
} catch (err) {
  console.error("Error", err)
}
