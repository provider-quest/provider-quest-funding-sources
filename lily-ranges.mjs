import { S3Client, paginateListObjectsV2 } from "@aws-sdk/client-s3"

export default async function lilyRanges () {
  const client = new S3Client({
    region: 'us-east-2'
  })
  const bucketParams = {
    Bucket: 'lily-data',
    Prefix: 'data/',
    Delimiter: '/'
  }
  let ranges = []
  for await (const data of paginateListObjectsV2({ client }, bucketParams)) {
    const pageRanges = data.CommonPrefixes.map(({ Prefix: prefix }) => {
      const match = prefix.match(/^data\/(\d+)_+(\d+)\/$/)
      return { from: Number(match[1]), to: Number(match[2]) }
    })
    ranges = ranges.concat(pageRanges)
  }
  ranges.sort(({ from: a }, { from: b }) => a - b)
  return ranges
}
