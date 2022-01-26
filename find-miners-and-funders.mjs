import fs from 'fs'
import { parse } from 'csv-parse'

const addressToId = new Map()
const idToAddress = new Map()
const addressFirstFunder = new Map()

async function parseIdAddresses () {
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
  })

  const file = 'sync/id-addresses/0000000000__0000000239.csv'

  const stream = fs.createReadStream(file)
  stream.pipe(parser)

  const promise = new Promise(resolve => {
    parser.on('end', () => {
      for (const epoch in epochs) {
        for (const { id, address } of epochs[epoch]) {
          console.log(`Address ${id} ${address} at ${epoch}`)
          addressToId.set(address, id)
          idToAddress.set(id, address)
        }
      }
      resolve()
    })
  })

  await promise
}

async function parseParsedMessages () {
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
  })

  const file = 'sync/parsed-messages/0000000000__0000000239.csv'

  const stream = fs.createReadStream(file)
  stream.pipe(parser)

  const promise = new Promise(resolve => {
    parser.on('end', () => {
      for (const epoch in epochs) {
        for (const { from, to } of epochs[epoch]) {
          if (!addressFirstFunder.get(to)) {
            addressFirstFunder.set(to, from)
            console.log(`First fund ${from} => ${to} at ${epoch}`)
          }
        }
      }
      resolve()
    })
  })

  await promise
}

async function parseMinerInfos () {
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
  })

  // const file = 'sync/parsed-messages/0000000000__0000000239.csv'
  const file = 'sync/miner-infos/0000000000__0000000239.csv'

  const stream = fs.createReadStream(file)
  stream.pipe(parser)

  const promise = new Promise(resolve => {
    parser.on('end', () => {
      for (const epoch in epochs) {
        for (const { minerId, ownerId } of epochs[epoch]) {
          console.log(`Miner ${minerId} at ${epoch}`)
          console.log(` Owner: ${ownerId} ${idToAddress.get(ownerId)}`)
        }
      }
      resolve()
    })
  })

  await promise
}

async function run () {
  await parseIdAddresses()
  await parseParsedMessages()
  await parseMinerInfos()
}
run()

