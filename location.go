package pixidb

import "math"

type Location interface{}

type IndexLocation int

type RingLocation int

type NestLocation int

type UniqueLocation int

type GridLocation struct {
	X int
	Y int
}

type SphericalLocation struct {
	Latitude  float64
	Longitude float64
}

type ProjectedLocation struct {
	X float64
	Y float64
}

type RectangularLocation struct {
	X float64
	Y float64
	Z float64
}

func (r RectangularLocation) ToSpherical() SphericalLocation {
	theta := math.Atan2(math.Sqrt(r.X*r.X+r.Y*r.Y), r.Z)
	phi := math.Atan2(r.Y, r.X)
	if phi < 0 {
		phi += 2 * math.Pi
	}
	if phi >= 2*math.Pi {
		phi -= 2 * math.Pi
	}
	return SphericalLocation{theta, phi}
}
