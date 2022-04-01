import fs from 'fs'
import { parse } from 'csv-parse'
import { epochToDate } from './filecoin-epochs.mjs'
import lilyDates from './lily-dates.mjs'
import syncLily from './sync-lily.mjs'
import Database from 'better-sqlite3'
import 'dotenv/config'
import minimist from 'minimist'

const addressToId = new Map()
const idToAddress = new Map()
const addressRegisteredEpoch = new Map()
const addressFunded = new Map()
const seenMiners = new Map()
const workDir = process.env.WORK_DIR || '.'
const argv = minimist(process.argv.slice(2), { boolean: true })

addressFunded.set('f1ojyfm5btrqq63zquewexr4hecynvq6yjyk5xv6q', null) // f0110 - genesis

fs.mkdirSync(`${workDir}/checkpoints`, { recursive: true })
fs.mkdirSync(`${workDir}/results`, { recursive: true })

async function parseIdAddresses (date) {
  const parser = parse()
  const epochs = []

  parser.on('readable', function () {
    let record
    while ((record = parser.read()) !== null) {
      const [
        height,
        id,
        address,
        state_root
      ] = record
      const epoch = Number(height)
      if (!epochs[epoch]) {
        epochs[epoch] = []
      }
      epochs[epoch].push({ id, address })
    }
  })

  parser.on('error', function (err) {
    console.error(err.message)
    process.exit(1)
  })

  const file = `${workDir}/sync/id-addresses/${date}.csv`

  const stream = fs.createReadStream(file)
  stream.pipe(parser)

  const promise = new Promise(resolve => {
    parser.on('end', () => {
      for (const epoch in epochs) {
        for (const { id, address } of epochs[epoch]) {
          // console.log(`Address ${id} ${address} at ${epoch}`)
          addressToId.set(address, id)
          addressRegisteredEpoch.set(address, Number(epoch))
          idToAddress.set(id, address)
        }
      }
      resolve()
    })
  })

  await promise
}

async function parseParsedMessages (date) {
  const parser = parse()
  const epochs = []

  parser.on('readable', function () {
    let record
    while ((record = parser.read()) !== null) {
      const [
        height,
        cid,
        from,
        to,
        value,
        method,
        params
      ] = record
      if (method === 'Send') {
        const epoch = Number(height)
        if (!epochs[epoch]) {
          epochs[epoch] = []
        }
        epochs[epoch].push({ from, to })
      }
    }
  })

  parser.on('error', function (err) {
    console.error(err.message)
    process.exit(1)
  })

  const file = `${workDir}/sync/parsed-messages/${date}.csv`

  const stream = fs.createReadStream(file)
  stream.pipe(parser)

  const promise = new Promise(resolve => {
    parser.on('end', () => {
      for (const epoch in epochs) {
        for (let { from, to } of epochs[epoch]) {
          if (from.match(/^f0/)) {
            const lookup = idToAddress.get(from)
            if (lookup) {
              from = lookup
            } else {
              console.error(`  Warning: Failed lookup for ${from}`)
            }
          }
          if (to.match(/^f0/)) {
            const lookup = idToAddress.get(to)
            if (lookup) {
              to = lookup
            } else {
              console.error(`  Warning: Failed lookup for ${to}`)
            }
          }
          if (!addressFunded.has(to)) {
            if (checkNoLoop(to, from)) {
              addressFunded.set(to, { from, epoch: Number(epoch) })
              // console.log(`First fund ${from} => ${to} at ${epoch}`)
            } else {
              console.error(`  Warning: Loop detected, skipping ${from} -> ${to}`)
            }
          }
        }
      }
      resolve()
    })
  })

  await promise
}

function checkNoLoop (to, from) {
  const seenSet = new Set()
  seenSet.add(to)
  let address = from
  let funded
  while (funded = addressFunded.get(address)) {
    const { from, epoch } = funded
    if (seenSet.has(from)) {
      return false
    }
    seenSet.add(address)
    address = from
  }
  return true
}

async function parseMinerInfos (date) {
  const parser = parse()
  const epochs = []

  parser.on('readable', function () {
    let record
    while ((record = parser.read()) !== null) {
      const [
        height,
        minerId,
        stateRoot,
        ownerId,
        workerId,
        newWorker,
        workerChangeEpoch,
        consensusFaultElapsed,
        peerId,
        controlAddress,
        multiAddress,
        sectorSize
      ] = record
      const epoch = Number(height)
      if (!epochs[epoch]) {
        epochs[epoch] = []
      }
      epochs[epoch].push({ minerId, ownerId })
    }
  })

  parser.on('error', function (err) {
    console.error(err.message)
    process.exit(1)
  })

  const file = `${workDir}/sync/miner-infos/${date}.csv`

  const stream = fs.createReadStream(file)
  stream.pipe(parser)

  const promise = new Promise(resolve => {
    parser.on('end', () => {
      for (const epoch in epochs) {
        const date = epochToDate(epoch)
        for (const { minerId, ownerId } of epochs[epoch]) {
          if (!seenMiners.has(minerId)) {
            // console.log(`Miner ${minerId} at ${epoch} - ${date}`)
            // console.log(` Owner: ${ownerId} ${idToAddress.get(ownerId)}`)
            let address = idToAddress.get(ownerId)
            let funded
            let lastEpoch = Number(epoch)
            let displayed = new Set()
            displayed.add(address)
            while(funded = addressFunded.get(address)) {
              const { from, epoch } = funded
              // console.log(`   @${epoch}: ${addressToId.get(from)} ${from} -> ${addressToId.get(address)} ${address}`)
              if (epoch > lastEpoch) {
                console.error(`      Warning: SP ${minerId}: Funded at future ${epoch} > ${lastEpoch}: ${addressToId.get(address)} ${address}`)
                // addressFunded.set(address, null) // Try to break cycles
                // break
              }
              if (displayed.has(from)) {
                console.error(`      Error: loop detected - ${minerId}`)
                break
              }
              address = from
              lastEpoch = epoch
            }
            seenMiners.set(minerId, {
              epoch,
              ownerId
            })
          }
        }
      }
      resolve()
    })
  })

  await promise
}

function writeCheckpointAndResults (date) {
  console.log('Writing checkpoint', date)
  const start = Date.now()
  const file = `${workDir}/checkpoints/${date}.db`
  try {
    if (fs.existsSync(`${file}.tmp`)) {
      fs.unlinkSync(`${file}.tmp`)
    }
    const db = new Database(`${file}.tmp`)

    db.exec(
      `CREATE TABLE IF NOT EXISTS addresses(` +
      `address VARCHAR, ` +
      `id VARCHAR, ` +
      `registered_epoch INT, ` +
      `funded_epoch INT, ` +
      `funded_from VARCHAR);`)

    const insertAddress = db.prepare(
      'INSERT INTO addresses ' +
      '(address, id, registered_epoch, funded_epoch, funded_from) ' +
      'VALUES (@address, @id, @registeredEpoch, @fundedEpoch, @fundedFrom)');

    const addressBatch = []

    const insertAddressTransaction = db.transaction(() => {
      if (addressBatch.length > 0) {
        for (const record of addressBatch) {
          insertAddress.run(record)
        }
      }
      addressBatch.length = 0
    })

    let counter = 0
    for (const [ address, id ] of addressToId) {
      let fundedEpoch
      let fundedFrom
      const registeredEpoch = addressRegisteredEpoch.get(address)
      const fundedRecord = addressFunded.get(address)
      if (fundedRecord) {
        fundedFrom = fundedRecord.from
        fundedEpoch = fundedRecord.epoch
      }
      /*
      console.log('Address', address, 'Id', id,
                  'registered_epoch', registeredEpoch,
                  'funded_epoch', fundedEpoch,
                  'funded_from', fundedFrom)
      */
      addressBatch.push({ address, id, registeredEpoch, fundedEpoch, fundedFrom })
      if (counter++ % 1000 === 0) {
        insertAddressTransaction()
      }
    }
    insertAddressTransaction()

    db.exec(
      `CREATE TABLE IF NOT EXISTS miners(` +
      `id VARCHAR, ` +
      `owner_id VARCHAR, ` +
      `epoch INT);`)

    const insertMiner = db.prepare(
      'INSERT INTO miners ' +
      '(id, owner_id, epoch) ' +
      'VALUES (@minerId, @ownerId, @epoch)');

    const minerBatch = []

    const insertMinerTransaction = db.transaction(() => {
      if (minerBatch.length > 0) {
        for (const record of minerBatch) {
          insertMiner.run(record)
        }
      }
      minerBatch.length = 0
    })

    for (const [ minerId, { ownerId, epoch } ] of seenMiners) {
      // console.log('Miner', minerId, 'OwnerId', ownerId, 'Epoch', epoch)
      minerBatch.push({ minerId, ownerId, epoch })
      if (counter++ % 1000 === 0) {
        insertMinerTransaction()
      }
    }
    insertMinerTransaction()

    const queryStmt = db.prepare(
      `
        SELECT addresses.id, address, funded_from, miners.id AS miner_id
        FROM addresses, miners
        WHERE miners.owner_id = addresses.id
        GROUP by addresses.id, address, funded_from, miner_id
      `)
    const queryRows = queryStmt.all()
    fs.writeFileSync(
      `${workDir}/results/miners-and-addresses-${date}.json`,
      JSON.stringify(queryRows)
    )

    const query2Stmt = db.prepare(
      `
        WITH RECURSIVE
          funded(id, address, funded_from, miner_id) AS (
            SELECT
               miner_address.id,
               miner_address.address,
               owner_address.address AS funded_from,
               miners.id AS miner_id
              FROM addresses AS miner_address, addresses AS owner_address, miners
                WHERE miners.id = miner_address.id
                  AND miners.owner_id = owner_address.id
              GROUP by miner_address.id, miner_address.address, owner_address.address, miner_id
            UNION
            SELECT addresses.id, addresses.address, addresses.funded_from, NULL AS miner_id
             FROM addresses
             INNER JOIN funded
               ON funded.funded_from = addresses.address
          )
        SELECT * FROM funded
      `)
    const query2Rows = query2Stmt.all()
    fs.writeFileSync(
      `${workDir}/results/miners-and-funders-${date}.json`,
      JSON.stringify(query2Rows)
    )

    db.close()
    fs.renameSync(`${file}.tmp`, file)
  } catch (e) {
    console.error('writeCheckpoint Exception', e)
  }
  console.log('Wrote checkpoint', date, (Date.now() - start) / 1000)
}

async function loadCheckpoint (checkpointFile) {
  const db = new Database(checkpointFile, { readonly: true })

  const addressesStmt = db.prepare('SELECT * FROM addresses')
  const addressesRows = addressesStmt.all()
  for (const row of addressesRows) {
    addressToId.set(row.address, row.id)
    idToAddress.set(row.id, row.address)
    addressRegisteredEpoch.set(row.address, Number(row.registered_epoch))
    if (row.funded_from && row.funded_epoch) {
      addressFunded.set(row.address, { from: row.funded_from, epoch: Number(row.funded_epoch) })
    }
  }

  const minersStmt = db.prepare('SELECT * FROM miners')
  const minersRows = minersStmt.all()
  for (const row of minersRows) {
    seenMiners.set(row.id, {
      epoch: Number(row.epoch),
      ownerId: row.owner_id
    })
  }

  db.close()
}

async function run () {
  fs.mkdirSync('checkpoints', { recursive: true })
  const availableDates = []
  if (argv.local) {
    const files = fs.readdirSync(`${workDir}/sync/parsed-messages`)
    for (const file of files) {
      const match = file.match(/(\d+-\d+-\d+)\.csv/)
      if (match) {
        const date = match[1]
        availableDates.push(date)
      }
    }
  } else {
    const dates = await lilyDates('miner_infos')
    for (const date of dates) {
      availableDates.push(date)
    }
  }
  console.log(`${availableDates.length} available dates, last: ${availableDates.slice(-1)[0]}`)
  availableDates.reverse()
  let lastCheckpoint
  const datesToProcess = []
  for (const date of availableDates) {
    const checkpointFile = `${workDir}/checkpoints/${date}.db`
    if (fs.existsSync(checkpointFile)) {
      lastCheckpoint = checkpointFile
      break
    }
    datesToProcess.unshift(date)
  }
  if (lastCheckpoint) {
    await loadCheckpoint(lastCheckpoint)
  }
  console.log('Last checkpoint:', lastCheckpoint)
  if (datesToProcess.length === 0) {
    console.log('No dates to process, all caught up.')
  } else {
    console.log(`${datesToProcess.length} dates to process, ${datesToProcess[0]} to ${datesToProcess.slice(-1)[0]}`)
  }
  for (const date of datesToProcess) {
    console.log('Date: ', date)
    await syncLily('id_addresses', date)
    await syncLily('miner_infos', date)
    await syncLily('parsed_messages', date)
    console.log('Processing...')
    await parseIdAddresses(date)
    await parseParsedMessages(date)
    await parseMinerInfos(date)
    if (!argv.monthly || date.slice(8) === '01' || date.slice(8) == '11' || date.slice(8) == '21') {
      await writeCheckpointAndResults(date)
    }
    if (argv.delete) {
      fs.unlinkSync(`${workDir}/sync/id-addresses/${date}.csv`)
      fs.unlinkSync(`${workDir}/sync/miner-infos/${date}.csv`)
      fs.unlinkSync(`${workDir}/sync/parsed-messages/${date}.csv`)
    }
  }
  console.log('Done.')
}

try {
  run()
} catch (e) {
  console.error('Exception:', e)
}

