import zarr from 'zarr-js'
import ndarray from 'ndarray'
import { distance } from '@turf/turf'
import { fromUrl } from 'geotiff'

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
  getValuesToSet,
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
    regionOptions,
    frag: customFrag,
    fillValue = -9999,
    mode = 'texture',
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
    this.regionOptions = regionOptions
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
        
        this.cogLoader = async (tiff, x, y, zoom, cb) => {
          // console.log('IN COG LOADER')
          // console.log(cb)
          // get file size
          const tiffHt = 2851;
          const tiffWd = 2841;      
          // you'll have square the factor number of tiles.
          // Zoom level 1 has 4 tiles, so we split the height in 2 and width in 2.
          // Zoom level 2 has 16 tiles so we split the height in 4 and width in 4.
          // Zoom level 3 has 64 tiles so we split the height in 8 and the width in 8.
          const factors = [1, 2, 4, 8, 16, 32]
          const factor = factors[zoom];
          const widthChunk = tiffWd / factor;
          const heightChunk = tiffHt / factor;
          // the current window should be offset by the width and height of the current chunk 
          // chunk 0.0 should be [0, 0, widthChunk, heightChunk]
          // chunk 0.1 should be [0, heightChunk, widthChunk, heightChunk * y]
          // to read the first window
          // const imgWindow = [ 0, 0, widthChunk, heightChunk] // xmin, ymin, xmax, ymax
          const xKey = x
          const yKey = y
          const xStart = xKey * widthChunk
          const yStart = yKey * heightChunk
          const xOffset = xStart + widthChunk
          const yOffset = yStart + heightChunk
          const imgWindow = [ xStart, yStart, xOffset, yOffset ];
          let response;
          await tiff.readRasters({
            window: imgWindow,
            width: 128,
            height: 128,
            resampleMethod: 'nearest'
          }).then((tiffData) => {
            const data = Float32Array.from(tiffData[0]);
            const key = [x, y, zoom].join(',');
            response = data;
          })
          console.log(response);
          return cb(null, response);
        }
        
        this.initialized = new Promise((resolve) => {
          fromUrl(source).then(async (tiff) => {
            console.log('tiff')
            console.log(tiff)
            //const levels = [0, 1, 2, 3, 4, 5];
            const levels = [0, 1];
            const maxZoom = 5;
            const image = await tiff.getImage();
            const tileWidth = image.getTileWidth();
            const tileHeight = image.getTileHeight();
            this.maxZoom = maxZoom
            const position = getPositions(tileHeight, mode)
            this.position = regl.buffer(position)
            this.size = tileHeight
            if (mode === 'grid' || mode === 'dotgrid') {
              this.count = position.length
            }
            if (mode === 'texture') {
              this.count = 6
            }
            this.dimensions = ['x', 'y'];
            this.shape = [tileWidth, tileHeight]
            this.chunks = this.shape
            
            this.ndim = this.dimensions.length;
            
            const fillErUp = levels.forEach(async (z) => {
              console.log('z')
              console.log(z)
              Array(Math.pow(2, z))
              .fill(0)
              .map((_, x) => {
                Array(Math.pow(2, z))
                .fill(0)
                .map(async (_, y) => {
                  const key = [x, y, z].join(',');
                  const loader = await this.cogLoader(tiff, x, y, z, (err, data) => {
                    console.log(data)
                    return data;
                  });
                  console.log('loader')
                  console.log(loader)
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
            console.log(fillErUp);
            Promise.all(fillErUp);
            
            resolve(true)
            this.invalidate()
          })

        // zarr().openGroup(source, (err, loaders, metadata) => {
        //   const { levels, maxZoom, tileSize } = getPyramidMetadata(metadata)
        //   this.maxZoom = maxZoom
        //   const position = getPositions(tileSize, mode)
        //   console.log('position');
        //   console.log(position);
        //   this.position = regl.buffer(position)
        //   this.size = tileSize
        //   console.log('this.size')
        //   console.log(this.size)
        //   if (mode === 'grid' || mode === 'dotgrid') {
        //     this.count = position.length
        //   }
        //   if (mode === 'texture') {
        //     this.count = 6
        //   }
        //   this.dimensions =
        //   metadata.metadata[`${levels[0]}/${variable}/.zattrs`][
        //     '_ARRAY_DIMENSIONS'
        //   ]
        //   console.log(this.dimensions)
        //   console.log('this.dimensions')
        //   this.shape =
        //   metadata.metadata[`${levels[0]}/${variable}/.zarray`]['shape']
        //   console.log('this.shape')
        //   console.log(this.shape)
        //   this.chunks =
        //   metadata.metadata[`${levels[0]}/${variable}/.zarray`]['chunks']
        //   console.log('this.chunks')
        //   console.log(this.chunks)
          
        //   this.ndim = this.dimensions.length
          
        //   this.coordinates = {}
        //   Promise.all(
        //     Object.keys(selector).map(
        //       (key) =>
        //       new Promise((innerResolve) => {
        //         loaders[`${levels[0]}/${key}`]([0], (err, chunk) => {
        //           const coordinates = Array.from(chunk.data)
        //           console.log('coordinates: ')
        //           console.log(coordinates)
        //           this.coordinates[key] = coordinates
        //           innerResolve()
        //         })
        //       })
        //       )
        //       ).then(() => {
        //         levels.forEach((z) => {
        //           const loader = loaders[z + '/' + variable]
        //           this.loaders[z] = loader
        //           console.log('loader')
        //           console.log(loader)
        //           console.log('z: ')
        //           console.log(z)
        //           Array(Math.pow(2, z))
        //           .fill(0)
        //           .map((_, x) => {
        //             Array(Math.pow(2, z))
        //             .fill(0)
        //             .map((_, y) => {
        //               const key = [x, y, z].join(',')
        //               this.tiles[key] = new Tile({
        //                 key,
        //                 loader,
        //                 shape: this.shape,
        //                 chunks: this.chunks,
        //                 dimensions: this.dimensions,
        //                 coordinates: this.coordinates,
        //                 bands: this.bands,
        //                 initializeBuffer: initialize,
        //               })
        //             })
        //           })
        //         })
                
        //         resolve(true)
        //         this.invalidate()
        //       })
        //   })          
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
                          
                          if (!tile.hasPopulatedBuffer(this.selector)) {
                            if (!tile.loading) {
                              if (tile.hasLoadedChunks(chunks)) {
                                tile.populateBuffersSync(this.selector)
                                this.invalidate()
                                resolve(false)
                              } else {
                                tile
                                .populateBuffers(chunks, this.selector, this.zoom)
                                .then((dataUpdated) => {
                                  this.invalidate()
                                  resolve(dataUpdated)
                                })
                              }
                            }
                          }
                        }
                      })
                      )
                      ).then((results) => {
                        if (results.some(Boolean)) {
                          invalidateRegion()
                        }
                      })
                    }
                    
                    this.queryRegion = async (region) => {
                      const tiles = getTilesOfRegion(region, this.level)
                      
                      await this.initialized
                      
                      if (this.regionOptions.loadAllChunks) {
                        await Promise.all(
                          tiles.map((key) => {
                            const tileIndex = keyToTile(key)
                            const chunks = getChunks(
                              {},
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
                          } else {
                            await Promise.all(tiles.map((key) => this.tiles[key].ready))
                          }
                          
                          let results,
                          lat = [],
                          lon = []
                          if (this.ndim > 2) {
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
                                      
                                      if (this.ndim > 2) {
                                        const valuesToSet = getValuesToSet(
                                          this.tiles[key].getData(),
                                          i,
                                          j,
                                          this.dimensions,
                                          this.coordinates
                                          )
                                          
                                          valuesToSet.forEach(({ keys, value }) => {
                                            setObjectValues(results, keys, value)
                                          })
                                        } else {
                                          results.push(data.get(j, i))
                                        }
                                      }
                                    }
                                  }
                                })
                                
                                const out = { [this.variable]: results }
                                
                                if (this.ndim > 2) {
                                  out.dimensions = [...Object.keys(this.coordinates), 'lat', 'lon']
                                  out.coordinates = { ...this.coordinates, lat, lon }
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
                          