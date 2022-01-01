import zarr from 'zarr-js'
import ndarray from 'ndarray'
import { distance } from '@turf/turf'
let GeoTIFF;
try {
  GeoTIFF = require('geotiff');
} catch (err) {
  GeoTIFF = null;
}

import { vert, frag } from './shaders'
import {
  zoomToLevel,
  keyToTile,
  pointToCamera,
  pointToTile,
  getPositions,
  getSiblings,
  getKeysToRender,
  getAdjustedOffset,
  getOverlappingAncestor,
  cameraToPoint,
  getTilesOfRegion,
  getPyramidMetadata,
  getBands,
  setObjectValues,
  getChunks,
} from './utils'
import Tile from './tile'

export const createTiles = (regl, opts) => {
  return new Tiles(opts)

  function Tiles({
    source,
    colormap,
    clim,
    opacity,
    display,
    variable,
    selector = {},
    uniforms: customUniforms = {},
    type = 'zarr',
    frag: customFrag,
    fillValue = -9999,
    mode = 'texture',
    setLoading,
    invalidate,
    invalidateRegion,
  }) {
    this.tiles = {}
    this.loaders = {}
    this.active = {}
    this.display = display
    this.clim = clim
    this.opacity = opacity
    this.selector = selector
    this.variable = variable
    this.fillValue = fillValue
    this.invalidate = invalidate
    this.viewport = { viewportHeight: 0, viewportWidth: 0 }
    this.type = type
    this._loading = false
    this._setLoading = setLoading
    this.colormap = regl.texture({
      data: colormap,
      format: 'rgb',
      shape: [colormap.length, 1],
    })

    const validModes = ['grid', 'dotgrid', 'texture']
    if (!validModes.includes(mode)) {
      throw Error(
        `mode '${mode}' invalid, must be one of ${validModes.join(', ')}`
      )
    }

    this.bands = getBands(variable, selector)

    customUniforms = Object.keys(customUniforms)

    let primitive,
      initialize,
      attributes = {},
      uniforms = {}

    if (mode === 'grid' || mode === 'dotgrid') {
      primitive = 'points'
      initialize = () => regl.buffer()
      this.bands.forEach((k) => (attributes[k] = regl.prop(k)))
      uniforms = {}
    }

    if (mode === 'texture') {
      primitive = 'triangles'
      const emptyTexture = ndarray(
        new Float32Array(Array(1).fill(fillValue)),
        [1, 1]
      )
      initialize = () => regl.texture(emptyTexture)
      this.bands.forEach((k) => (uniforms[k] = regl.prop(k)))
    }

    customUniforms.forEach((k) => (uniforms[k] = regl.this(k)))

    this.loadCog = (source, resolve) => {
      GeoTIFF.fromUrl(source).then(async (tiff) => {
        const image = await tiff.getImage();
        const width = image.getWidth();
        const height = image.getHeight();
        const tileWidth = image.getTileWidth();
        const tileHeight = image.getTileHeight();
        // Assumption
        const tileSize = tileWidth;
        // Review: How do we want
        const imageCount = await tiff.getImageCount();
        const levels = Array.from(Array(imageCount).keys());
        // you'll have square the factor number of tiles.
        // Zoom level 1 has 4 tiles, so we split the height in 2 and width in 2.
        // Zoom level 2 has 16 tiles so we split the height in 4 and width in 4.
        // Zoom level 3 has 64 tiles so we split the height in 8 and the width in 8.
        const factors = levels.map(z => Math.pow(2, z));

        this.maxZoom = imageCount - 1;
        const position = getPositions(tileSize, mode);
        this.position = regl.buffer(position)
        this.size = tileSize
        if (mode === 'grid' || mode === 'dotgrid') {
          this.count = position.length
        }
        if (mode === 'texture') {
          this.count = 6
        }
        // in future versions this might have a selector for different bands
        this.dimensions = ['x', 'y'];
        this.shape = [tileWidth, tileHeight];
        this.chunks = this.shape;
        this.ndim = this.dimensions.length

        this.coordinates = {}

        Promise.all(
          Object.keys(selector).map(
            (key) =>
            new Promise((innerResolve) => {
              loaders[`${levels[0]}/${key}`]([0], (err, chunk) => {
                const coordinates = Array.from(chunk.data)
                this.coordinates[key] = coordinates
                innerResolve()
              })
            })
            )
            ).then(() => {
              levels.forEach((z) => {
                Array(Math.pow(2, z))
                .fill(0)
                .map((_, x) => {
                  Array(Math.pow(2, z))
                  .fill(0)
                  .map((_, y) => {
                    const key = [x, y, z].join(',');

                    const factor = factors[z];
                    const widthChunk = width / factor;
                    const heightChunk = height / factor;
                    const xStart = x * widthChunk;
                    const yStart = y * heightChunk;
                    const xOffset = xStart + widthChunk
                    const yOffset = yStart + heightChunk
                    const imgWindow = [ xStart, yStart, xOffset, yOffset ];
                    const loader = async (k, cb) => {
                      let data;
                      await tiff.readRasters({
                        window: imgWindow,
                        width: tileWidth,
                        height: tileHeight,
                        resampleMethod: 'nearest'
                      }).then((tiffData) => {
                        data = ndarray(Float32Array.from(tiffData[0]), this.shape)
                      })
                      return cb(null, data)
                    };
                    this.loaders[z] = loader;

                    this.tiles[key] = new Tile({
                      key,
                      loader,
                      shape: this.shape,
                      chunks: this.chunks,
                      dimensions: this.dimensions,
                      coordinates: this.coordinates,
                      bands: this.bands,
                      initializeBuffer: initialize,
                    })
                  })
                })
              })

              resolve(true)
              this.invalidate()
            })
          })
        }

    this.initialized = new Promise((resolve) => {
      if (this.type === 'cog') {
        return this.loadCog(source, resolve)
      }
      zarr().openGroup(source, (err, loaders, metadata) => {
        const { levels, maxZoom, tileSize } = getPyramidMetadata(metadata)
        this.maxZoom = maxZoom
        const position = getPositions(tileSize, mode)
        this.position = regl.buffer(position)
        this.size = tileSize
        if (mode === 'grid' || mode === 'dotgrid') {
          this.count = position.length
        }
        if (mode === 'texture') {
          this.count = 6
        }
        this.dimensions =
          metadata.metadata[`${levels[0]}/${variable}/.zattrs`][
            '_ARRAY_DIMENSIONS'
          ]
        this.shape =
          metadata.metadata[`${levels[0]}/${variable}/.zarray`]['shape']
        this.chunks =
          metadata.metadata[`${levels[0]}/${variable}/.zarray`]['chunks']

        this.ndim = this.dimensions.length

        this.coordinates = {}
        Promise.all(
          Object.keys(selector).map(
            (key) =>
              new Promise((innerResolve) => {
                loaders[`${levels[0]}/${key}`]([0], (err, chunk) => {
                  const coordinates = Array.from(chunk.data)
                  this.coordinates[key] = coordinates
                  innerResolve()
                })
              })
          )
        ).then(() => {
          levels.forEach((z) => {
            const loader = loaders[z + '/' + variable]
            this.loaders[z] = loader
            Array(Math.pow(2, z))
              .fill(0)
              .map((_, x) => {
                Array(Math.pow(2, z))
                  .fill(0)
                  .map((_, y) => {
                    const key = [x, y, z].join(',')
                    this.tiles[key] = new Tile({
                      key,
                      loader,
                      shape: this.shape,
                      chunks: this.chunks,
                      dimensions: this.dimensions,
                      coordinates: this.coordinates,
                      bands: this.bands,
                      initializeBuffer: initialize,
                    })
                  })
              })
          })

          resolve(true)
          this.invalidate()
        })
      })
    })

    this.drawTiles = regl({
      vert: vert(mode, this.bands),

      frag: frag(mode, this.bands, customFrag, customUniforms),

      attributes: {
        position: regl.this('position'),
        ...attributes,
      },

      uniforms: {
        viewportWidth: regl.context('viewportWidth'),
        viewportHeight: regl.context('viewportHeight'),
        pixelRatio: regl.context('pixelRatio'),
        colormap: regl.this('colormap'),
        camera: regl.this('camera'),
        size: regl.this('size'),
        zoom: regl.this('zoom'),
        globalLevel: regl.this('level'),
        level: regl.prop('level'),
        offset: regl.prop('offset'),
        clim: regl.this('clim'),
        opacity: regl.this('opacity'),
        fillValue: regl.this('fillValue'),
        ...uniforms,
      },

      blend: {
        enable: true,
        func: {
          src: 'one',
          srcAlpha: 'one',
          dstRGB: 'one minus src alpha',
          dstAlpha: 'one minus src alpha',
        },
      },

      depth: { enable: false },

      count: regl.this('count'),

      primitive: primitive,
    })

    this.getProps = () => {
      const adjustedActive = Object.keys(this.tiles)
        .filter((key) => this.active[key])
        .reduce((accum, key) => {
          const keysToRender = getKeysToRender(key, this.tiles, this.maxZoom)
          keysToRender.forEach((keyToRender) => {
            const offsets = this.active[key]

            offsets.forEach((offset) => {
              const adjustedOffset = getAdjustedOffset(offset, keyToRender)
              if (!accum[keyToRender]) {
                accum[keyToRender] = []
              }

              const alreadySeenOffset = accum[keyToRender].find(
                (prev) =>
                  prev[0] === adjustedOffset[0] && prev[1] === adjustedOffset[1]
              )
              if (!alreadySeenOffset) {
                accum[keyToRender].push(adjustedOffset)
              }
            })
          })

          return accum
        }, {})

      const activeKeys = Object.keys(adjustedActive)

      return activeKeys.reduce((accum, key) => {
        if (!getOverlappingAncestor(key, activeKeys)) {
          const [, , level] = keyToTile(key)
          const tile = this.tiles[key]
          const offsets = adjustedActive[key]

          offsets.forEach((offset) => {
            accum.push({
              ...tile.getBuffers(),
              level,
              offset,
            })
          })
        }

        return accum
      }, [])
    }

    regl.frame(({ viewportHeight, viewportWidth }) => {
      if (
        this.viewport.viewportHeight !== viewportHeight ||
        this.viewport.viewportWidth !== viewportWidth
      ) {
        this.viewport = { viewportHeight, viewportWidth }
        this.invalidate()
      }
    })

    this.draw = () => {
      this.drawTiles(this.getProps())
    }

    this.setLoading = (value) => {
      if (!this._setLoading || value === this._loading) {
        return
      } else {
        this._loading = value
        this._setLoading(value)
      }
    }

    this.updateCamera = ({ center, zoom }) => {
      const level = zoomToLevel(zoom, this.maxZoom)
      const tile = pointToTile(center.lng, center.lat, level)
      const camera = pointToCamera(center.lng, center.lat, level)

      this.level = level
      this.zoom = zoom
      this.camera = [camera[0], camera[1]]

      this.active = getSiblings(tile, {
        viewport: this.viewport,
        zoom,
        camera: this.camera,
        size: this.size,
      })

      Promise.all(
        Object.keys(this.active).map(
          (key) =>
            new Promise((resolve) => {
              if (this.loaders[level]) {
                const tileIndex = keyToTile(key)
                const tile = this.tiles[key]

                const chunks = getChunks(
                  this.selector,
                  this.dimensions,
                  this.coordinates,
                  this.shape,
                  this.chunks,
                  tileIndex[0],
                  tileIndex[1]
                )
                if (tile.hasPopulatedBuffer(this.selector) || tile.loading) {
                  resolve(false)
                  return
                }

                if (tile.hasLoadedChunks(chunks)) {
                  tile.populateBuffersSync(this.selector)
                  this.invalidate()
                  resolve(false)
                } else {
                  // Set loading=true if any tile data is not yet fetched
                  this.setLoading(true)
                  tile
                    .populateBuffers(chunks, this.selector)
                    .then((dataUpdated) => {
                      this.invalidate()
                      resolve(dataUpdated)
                    })
                }
              }
            })
        )
      ).then((results) => {
        if (results.some(Boolean)) {
          invalidateRegion()
        }

        if (Object.keys(this.active).every((key) => !this.tiles[key].loading)) {
          // Set loading=false only when all active tiles are done loading
          this.setLoading(false)
        }
      })
    }

    this.queryRegion = async (region, selector) => {
      await this.initialized

      const tiles = getTilesOfRegion(region, this.level)

      await Promise.all(
        tiles.map((key) => {
          const tileIndex = keyToTile(key)
          const chunks = getChunks(
            selector,
            this.dimensions,
            this.coordinates,
            this.shape,
            this.chunks,
            tileIndex[0],
            tileIndex[1]
          )

          return this.tiles[key].loadChunks(chunks)
        })
      )

      let results,
        lat = [],
        lon = []
      const resultDim =
        this.ndim -
        Object.keys(selector).filter((k) => !Array.isArray(selector[k])).length
      if (resultDim > 2) {
        results = {}
      } else {
        results = []
      }

      tiles.map((key) => {
        const [x, y, z] = keyToTile(key)
        const { center, radius, units } = region.properties
        for (let i = 0; i < this.size; i++) {
          for (let j = 0; j < this.size; j++) {
            const pointCoords = cameraToPoint(
              x + i / this.size,
              y + j / this.size,
              z
            )
            const distanceToCenter = distance(
              [center.lng, center.lat],
              pointCoords,
              {
                units,
              }
            )
            if (distanceToCenter < radius) {
              lon.push(pointCoords[0])
              lat.push(pointCoords[1])

              const valuesToSet = this.tiles[key].getPointValues({
                selector,
                point: [i, j],
              })

              valuesToSet.forEach(({ keys, value }) => {
                if (keys.length > 0) {
                  setObjectValues(results, keys, value)
                } else {
                  results.push(value)
                }
              })
            }
          }
        }
      })

      const out = { [this.variable]: results }

      if (this.ndim > 2) {
        out.dimensions = this.dimensions.map((d) => {
          if (d === 'x') {
            return 'lon'
          } else if (d === 'y') {
            return 'lat'
          } else {
            return d
          }
        })

        out.coordinates = this.dimensions.reduce(
          (coords, d) => {
            if (d !== 'x' && d !== 'y') {
              if (selector.hasOwnProperty(d)) {
                coords[d] = Array.isArray(selector[d])
                  ? selector[d]
                  : [selector[d]]
              } else {
                coords[d] = this.coordinates[d]
              }
            }

            return coords
          },
          { lat, lon }
        )
      } else {
        out.dimensions = ['lat', 'lon']
        out.coordinates = { lat, lon }
      }

      return out
    }

    this.updateSelector = ({ selector }) => {
      this.selector = selector
      this.invalidate()
    }

    this.updateUniforms = (props) => {
      Object.keys(props).forEach((k) => {
        this[k] = props[k]
      })
      if (!this.display) {
        this.opacity = 0
      }
      this.invalidate()
    }

    this.updateColormap = ({ colormap }) => {
      this.colormap = regl.texture({
        data: colormap,
        format: 'rgb',
        shape: [colormap.length, 1],
      })
      this.invalidate()
    }
  }
}
