import { S3Client, paginateListObjectsV2 } from "@aws-sdk/client-s3"

export default async function lilyDates (table) {
  const dirPrefix = `mainnet/csv/1/${table}/`
  const client = new S3Client({
    region: 'us-east-2'
  })

  const years = await fetchYears(client, dirPrefix)

  let dates = []
  for (const year of years) {
    const newDates = await fetchDatesForYear(client, dirPrefix, table, year)
    dates = dates.concat(newDates)
  }

  return dates
}

export async function fetchYears (client, dirPrefix) {
  const bucketParams = {
    Bucket: 'fil-archive',
    Prefix: dirPrefix,
    Delimiter: '/'
  }
  const years = []
  for await (const data of paginateListObjectsV2({ client }, bucketParams)) {
    const pagedYears = data.CommonPrefixes
    for (const { Prefix: prefix } of pagedYears) {
      const regexStr = `^${dirPrefix}(\\d+)/$`
      const regex = new RegExp(regexStr)
      const match = prefix.match(regex)
      // console.log(prefix, regexStr, match)
      if (match) {
        years.push(Number(match[1]))
      }
    }
  }
  years.sort()
  return years
}

export async function fetchDatesForYear (client, dirPrefix, table, year) {
  const bucketParams = {
    Bucket: 'fil-archive',
    Prefix: `${dirPrefix}${year}/`,
    Delimiter: '/'
  }
  const dates = []
  for await (const data of paginateListObjectsV2({ client }, bucketParams)) {
    const contents = data.Contents
    for (const { Key: key } of contents) {
      // eg. messages-2020-12-31.csv.gz
      const regexStr = `^${dirPrefix}${year}/${table}-(\\d+)-(\\d+)-(\\d+)\\.csv\\.gz$`
      const regex = new RegExp(regexStr)
      const match = key.match(regex)
      // console.log(key, regexStr, match)
      if (match) {
        dates.push(new Date(`${match[1]}-${match[2]}-${match[3]}`))
      }
    }
  }
  dates.sort((a, b) => a.getTime() - b.getTime())
  return dates.map(date => date.toISOString().slice(0, 10))
}
