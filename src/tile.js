import ndarray from 'ndarray'
import {
  getBandInformation,
  getChunks,
  keyToTile,
  getSelectorHash,
} from './utils'
import { fromUrl } from 'geotiff';

class Tile {
  constructor({
    key,
    loader,
    shape,
    chunks,
    dimensions,
    coordinates,
    bands,
    initializeBuffer,
  }) {
    this.key = key
    this.tileCoordinates = keyToTile(key)

    this.loading = false
    this.shape = shape
    this.chunks = chunks
    this.dimensions = dimensions
    this.coordinates = coordinates
    this.bands = bands

    this._bufferCache = null
    this._buffers = {}

    bands.forEach((k) => {
      this._buffers[k] = initializeBuffer()
    })

    this.chunkedData = {}

    this._data = {
      chunkKeys: [],
      value: null,
    }
    this._loader = loader
    this._ready = false
    this._resetReady()
  }

  ready() {
    return this._ready
  }

  _setReady() {
    this._resolver(true)
  }

  _resetReady() {
    this._ready = new Promise((resolve) => {
      this._resolver = resolve
    })
  }

  getBuffers() {
    return this._buffers
  }

  async loadChunks(chunks, zoom) {
    this.loading = true
    this._resetReady()
    console.log(chunks);
    const updated = await Promise.all(
      chunks.map(
        (chunk) =>
          new Promise((resolve) => {
            const key = chunk.join('.')
            const xKey = chunk[1]
            const yKey = chunk[0]
            if (this.chunkedData[key]) {
              resolve(false)
            } else {
              console.log(`key is ${key}`)
              this._loader(chunk, (err, data) => {
                const file = "https://ds-data-projects.s3.amazonaws.com/smce-eis/3B-MO.MS.MRG.3IMERG.20200501-S000000-E235959.05.V06B.HDF5.tif";
                const tiffHt = 2851;
                const tiffWd = 2841;
                // you'll have twice as many tiles as the factor:
                // Zoom level 1 has a factor of 2 and 4 tiles
                // Zoom level 2 has a factor of 4 and 16 tiles, etc.
                const factors = [1, 2, 4, 8, 16, 32]
                console.log('zoom')
                console.log(zoom)
                const factor = factors[zoom - 1]
                // for zoom 1
                const widthChunk = 2841 / (factor || 1);
                console.log(widthChunk)
                const heightChunk = 2851 / (factor || 1);
                console.log(heightChunk)
                // the current window should be offset by the width and height of the current chunk 
                // chunk 0.0 should be [0, 0, widthChunk, heightChunk]
                // chunk 0.1 should be [0, heightChunk, widthChunk, heightChunk * y]
                // to read the first window
                // const imgWindow = [ 0, 0, widthChunk, heightChunk] // xmin, ymin, xmax, ymax
                const xStart = xKey * widthChunk
                const yStart = yKey * heightChunk
                const xOffset = xStart + widthChunk
                const yOffset = yStart + heightChunk
                const imgWindow = [ xStart, yStart, xOffset, yOffset ];
                console.log(imgWindow);
                // issue now is just that we're reading from part of the image that doesn't exist so at higher resolution zooms just need to read from the original image...
                fromUrl(file).then((tiff) => {
                    //console.log(tiff)
                    tiff.readRasters({
                      window: imgWindow,
                      width: 128,
                      height: 128,
                      resampleMethod: 'bilinear'
                    }).then((tiffData) => {
                      data.data = Float32Array.from(tiffData[0])
                      this.chunkedData[key] = data
                      resolve(true)
                    })
                });
              })

              // this._loader(chunk, (err, data) => {
              //   this.chunkedData[key] = data
              //   resolve(true)
              // })
            }
          })
      )
    )
    this._setReady(true)
    this.loading = false

    return updated.some(Boolean)
  }

  async populateBuffers(chunks, selector, zoom) {
    const updated = await this.loadChunks(chunks, Math.round(zoom))

    this.populateBuffersSync(selector)

    return updated
  }

  populateBuffersSync(selector) {
    const bandInformation = getBandInformation(selector)

    this.bands.forEach((band) => {
      const info = bandInformation[band] || selector
      const chunks = getChunks(
        info,
        this.dimensions,
        this.coordinates,
        this.shape,
        this.chunks,
        this.tileCoordinates[0],
        this.tileCoordinates[1]
      )
      const chunk = chunks[0]
      const chunkKey = chunk.join('.')
      const data = this.chunkedData[chunkKey]

      if (!data) {
        throw new Error(`Missing data for chunk: ${chunkKey}`)
      }
      if (info) {
        const indices = this.dimensions
          .map((d) => (['x', 'y'].includes(d) ? null : d))
          .map((d, i) => {
            if (info[d] === undefined) {
              return null
            } else {
              const value = info[d]
              return (
                this.coordinates[d].findIndex(
                  (coordinate) => coordinate === value
                ) % this.chunks[i]
              )
            }
          })

        this._buffers[band](data.pick(...indices))
      } else {
        this._buffers[band](data)
      }
    })

    this._bufferCache = getSelectorHash(selector)
  }

  isBufferPopulated() {
    return !!this._bufferCache
  }

  hasLoadedChunks(chunks) {
    return chunks.every((chunk) => this.chunkedData[chunk.join('.')])
  }

  hasPopulatedBuffer(selector) {
    return this._bufferCache && this._bufferCache === getSelectorHash(selector)
  }

  getData() {
    const keys = Object.keys(this.chunkedData)
    const keysToAdd = keys.filter((key) => !this._data.chunkKeys.includes(key))

    if (keysToAdd.length === 0) {
      return this._data.value
    }

    let data = this._data.value
    if (!data) {
      const size = this.shape.reduce((product, el) => product * el, 1)
      data = ndarray(new Float32Array(size), this.shape)
    }

    keysToAdd.forEach((key) => {
      const chunk = key.split('.')
      const chunkData = this.chunkedData[key]
      const result = this.chunks.reduce(
        (accum, count, i) => {
          const chunkOffset = ['x', 'y'].includes(this.dimensions[i])
            ? 0
            : chunk[i] * count
          let updatedAccum = []
          for (let j = 0; j < count; j++) {
            const index = chunkOffset + j
            updatedAccum = updatedAccum.concat(
              accum.map((prev) => [...prev, index])
            )
          }
          return updatedAccum
        },
        [[]]
      )

      result.forEach((indices) => {
        const chunkIndices = indices.map((el, i) =>
          ['x', 'y'].includes(this.dimensions[i])
            ? el
            : el - chunk[i] * this.chunks[i]
        )
        const value = chunkData.get(...chunkIndices)
        data.set(...indices, value)
      })
    })

    this._data.chunkKeys = keys
    this._data.value = data

    return data
  }
}

export default Tile
