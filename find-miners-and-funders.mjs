import fs from 'fs'
import { parse } from 'csv-parse'
import { epochToDate } from './filecoin-epochs.mjs'

const addressToId = new Map()
const idToAddress = new Map()
const addressFunded = new Map()
const seenMiners = new Set()

addressFunded.set('f1ojyfm5btrqq63zquewexr4hecynvq6yjyk5xv6q', null)

async function parseIdAddresses (range) {
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

  const file = `sync/id-addresses/${range}.csv`

  const stream = fs.createReadStream(file)
  stream.pipe(parser)

  const promise = new Promise(resolve => {
    parser.on('end', () => {
      for (const epoch in epochs) {
        for (const { id, address } of epochs[epoch]) {
          // console.log(`Address ${id} ${address} at ${epoch}`)
          addressToId.set(address, id)
          idToAddress.set(id, address)
        }
      }
      resolve()
    })
  })

  await promise
}

async function parseParsedMessages (range) {
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

  const file = `sync/parsed-messages/${range}.csv`

  const stream = fs.createReadStream(file)
  stream.pipe(parser)

  const promise = new Promise(resolve => {
    parser.on('end', () => {
      for (const epoch in epochs) {
        for (const { from, to } of epochs[epoch]) {
          if (!addressFunded.has(to)) {
            addressFunded.set(to, { from, epoch })
            // console.log(`First fund ${from} => ${to} at ${epoch}`)
          }
        }
      }
      resolve()
    })
  })

  await promise
}

async function parseMinerInfos (range) {
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

  const file = `sync/miner-infos/${range}.csv`

  const stream = fs.createReadStream(file)
  stream.pipe(parser)

  const promise = new Promise(resolve => {
    parser.on('end', () => {
      for (const epoch in epochs) {
        const date = epochToDate(epoch)
        for (const { minerId, ownerId } of epochs[epoch]) {
          if (!seenMiners.has(minerId)) {
            console.log(`Miner ${minerId} at ${epoch} - ${date}`)
            console.log(` Owner: ${ownerId} ${idToAddress.get(ownerId)}`)
            let address = idToAddress.get(ownerId)
            let funded
            while(funded = addressFunded.get(address)) {
              const { from, epoch } = funded
              console.log(`   Funded at ${epoch}: ${addressToId.get(from)} ${from}`)
              address = from
            }
            seenMiners.add(minerId)
          }
        }
      }
      resolve()
    })
  })

  await promise
}

async function run () {
  const files = fs.readdirSync('sync/parsed-messages')
  for (const file of files) {
    const match = file.match(/(\d+)__(\d+)\.csv/)
    if (match) {
      const range = `${match[1]}__${match[2]}`
      // console.log('Range: ', range)
      await parseIdAddresses(range)
      await parseParsedMessages(range)
      await parseMinerInfos(range)
    }
  }
}
run()

