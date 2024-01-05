package pixidb

import (
	"github.com/owlpinetech/flatsphere"
	"github.com/owlpinetech/healpix"
)

// Common functionality for converting between various different coordinate systems and
// pixel indices within a store.
type LocationIndexer interface {
	ToIndex(Location) (int, error)
	Projection() flatsphere.Projection
	Name() string
	Size() int
}

// Simple indexing into a grid, no spherical projection provided by this indexer. Supports
// either row-major or column-major storage of the data for particular access patterns.
type ProjectionlessIndexer struct {
	Width    int  `json:"width"`
	Height   int  `json:"heigh"`
	RowMajor bool `json:"rowmajor"`
}

func NewProjectionlessIndexer(width int, height int, rowMajor bool) ProjectionlessIndexer {
	return ProjectionlessIndexer{
		Width:    width,
		Height:   height,
		RowMajor: rowMajor,
	}
}

func (p ProjectionlessIndexer) Name() string {
	return "projectionless"
}

func (p ProjectionlessIndexer) Projection() flatsphere.Projection {
	return nil
}

func (p ProjectionlessIndexer) Size() int {
	return p.Width * p.Height
}

func (p ProjectionlessIndexer) ToIndex(loc Location) (int, error) {
	switch val := loc.(type) {
	case IndexLocation:
		return int(val), nil
	case GridLocation:
		if p.RowMajor {
			return val.Y*p.Width + val.X, nil
		}
		return val.X*p.Height + val.Y, nil
	default:
		return -1, NewLocationNotSupportedError(p.Name(), loc)
	}
}

// Indexing into a sphere of pixels project via a standard Mercator projection. Because
// Mercator diverges at the poles, two cutoff parameters are provided for the northern
// and southern latitudes. These cutoff parallels will mark the boundaries of the top
// and bottom of the grid respectively. Supports either row-major or column-major storage
// of the data for particular access patterns.
type MercatorCutoffIndexer struct {
	NorthCutoff  float64 `json:"northCutoff"`
	SouthCutoff  float64 `json:"southCutoff"`
	southProj    float64 // precomputed projected south latitude
	latRangeProj float64 // precomputed (North - South) latitude projected range
	grid         ProjectionlessIndexer
	proj         flatsphere.Mercator
}

func NewMercatorCutoffIndexer(northCutoff float64, southCutoff float64, width int, height int, rowMajor bool) MercatorCutoffIndexer {
	if northCutoff <= southCutoff {
		panic("pixidb: mercator north cutoff smaller than south cutoff")
	}
	proj := flatsphere.NewMercator()
	_, southY := proj.Project(southCutoff, 0)
	_, northY := proj.Project(northCutoff, 0)
	return MercatorCutoffIndexer{
		NorthCutoff:  northCutoff,
		SouthCutoff:  southCutoff,
		southProj:    southY,
		latRangeProj: northY - southY,
		grid:         NewProjectionlessIndexer(width, height, rowMajor),
		proj:         flatsphere.NewMercator(),
	}
}

func (m MercatorCutoffIndexer) Name() string {
	return "mercator-cutoff"
}

func (m MercatorCutoffIndexer) Projection() flatsphere.Projection {
	return m.proj
}

func (m MercatorCutoffIndexer) Size() int {
	return m.grid.Size()
}

func (m MercatorCutoffIndexer) ToIndex(loc Location) (int, error) {
	switch val := loc.(type) {
	case IndexLocation:
		return int(val), nil
	case GridLocation:
		return m.grid.ToIndex(loc)
	case SphericalLocation:
		if val.Latitude > m.NorthCutoff || val.Latitude < m.SouthCutoff {
			return -1, NewLocationOutOfBoundsError(loc)
		}
		x, y := m.proj.Project(val.Latitude, val.Longitude)
		return m.ToIndex(ProjectedLocation{x, y})
	case ProjectedLocation:
		bounds := m.proj.PlanarBounds()
		xPix := ((val.X - bounds.XMin) / bounds.Width()) * float64(m.grid.Width-1)
		yPix := ((val.Y - m.southProj) / m.latRangeProj) * float64(m.grid.Height-1)
		return m.ToIndex(GridLocation{int(xPix), int(yPix)})
	case RectangularLocation:
		return m.ToIndex(val.ToSpherical())
	default:
		return -1, NewLocationNotSupportedError(m.Name(), loc)
	}
}

// Indexing into a sphere of pixels projected via a cylindrical equirectangular projection.
// 0,0 is the bottom left corner of the projection space, i.e. (XMin, YMin) => (0, 0). Supports
// both row-major and column-major order of the grid, which changes how efficient certain
// consecutive x- or y-accesses are, but does not change where x,y coordinates refer to.
type CylindricalEquirectangularIndexer struct {
	Parallel float64 `json:"parallel"`
	grid     ProjectionlessIndexer
	proj     flatsphere.Equirectangular
}

// Create a new indexer into a grid with the cylindrical equirectangular projection, focused at
// the given latitude. Many common projections can be created this way.
func NewCylindricalEquirectangularIndexer(parallel float64, width int, height int, rowMajor bool) CylindricalEquirectangularIndexer {
	return CylindricalEquirectangularIndexer{
		Parallel: parallel,
		grid:     NewProjectionlessIndexer(width, height, rowMajor),
		proj:     flatsphere.NewEquirectangular(parallel),
	}
}

func (c CylindricalEquirectangularIndexer) Name() string {
	return "cylindrical-equirectangular"
}

func (c CylindricalEquirectangularIndexer) Projection() flatsphere.Projection {
	return c.proj
}

func (c CylindricalEquirectangularIndexer) Size() int {
	return c.grid.Size()
}

func (c CylindricalEquirectangularIndexer) ToIndex(loc Location) (int, error) {
	switch val := loc.(type) {
	case IndexLocation:
		return int(val), nil
	case GridLocation:
		return c.grid.ToIndex(loc)
	case SphericalLocation:
		x, y := c.proj.Project(val.Latitude, val.Longitude)
		return c.ToIndex(ProjectedLocation{x, y})
	case ProjectedLocation:
		bounds := c.proj.PlanarBounds()
		xPix := ((val.X - bounds.XMin) / bounds.Width()) * float64(c.grid.Width-1)
		yPix := ((val.Y - bounds.YMin) / bounds.Height()) * float64(c.grid.Height-1)
		return c.ToIndex(GridLocation{int(xPix), int(yPix)})
	case RectangularLocation:
		return c.ToIndex(val.ToSpherical())
	default:
		return -1, NewLocationNotSupportedError(c.Name(), loc)
	}
}

// Pixelizes a sphere using the HEALPix pixelisation method. This indexer promises a
// single resolution pixelization, where every pixel has the same angular area. Provides
// storage options of both ring and nested schemes, for making certain data-access patterns
// more efficient.
type FlatHealpixIndexer struct {
	Scheme healpix.HealpixScheme `json:"scheme"`
	Order  healpix.HealpixOrder  `json:"order"`
	proj   flatsphere.HEALPixStandard
}

func NewFlatHealpixIndexer(order healpix.HealpixOrder, scheme healpix.HealpixScheme) FlatHealpixIndexer {
	return FlatHealpixIndexer{
		Scheme: scheme,
		Order:  order,
		proj:   flatsphere.NewHEALPixStandard(),
	}
}

func (h FlatHealpixIndexer) Name() string {
	return "flat-healpix"
}

func (h FlatHealpixIndexer) Projection() flatsphere.Projection {
	return h.proj
}

func (h FlatHealpixIndexer) Size() int {
	return h.Order.Pixels()
}

func (h FlatHealpixIndexer) ToIndex(loc Location) (int, error) {
	switch val := loc.(type) {
	case IndexLocation:
		return int(val), nil
	case RingLocation:
		return healpix.RingPixel(int(val)).PixelId(h.Order, h.Scheme), nil
	case NestLocation:
		return healpix.NestPixel(int(val)).PixelId(h.Order, h.Scheme), nil
	case UniqueLocation:
		return healpix.UniquePixel(int(val)).PixelId(h.Order, h.Scheme), nil
	case SphericalLocation:
		return healpix.NewLatLonCoordinate(val.Latitude, val.Longitude).PixelId(h.Order, h.Scheme), nil
	case ProjectedLocation:
		return healpix.NewProjectionCoordinate(val.X, val.Y).PixelId(h.Order, h.Scheme), nil
	case RectangularLocation:
		return h.ToIndex(val.ToSpherical())
	default:
		return -1, NewLocationNotSupportedError(h.Name(), loc)
	}
}

// TODO: example of how to get sinusoidal into a grid
// https://modis-land.gsfc.nasa.gov/MODLAND_grid.html
