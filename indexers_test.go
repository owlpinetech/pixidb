package pixidb

import (
	"errors"
	"math"
	"testing"
)

func TestProjectionlessIndexerGrid(t *testing.T) {
	testCases := []struct {
		name     string
		width    int
		height   int
		rowMajor bool
	}{
		{"square row", 50, 50, true},
		{"square column", 53, 53, false},
		{"rect wide row", 50, 25, true},
		{"rect wide column", 53, 24, false},
		{"rect tall row", 25, 50, true},
		{"rect tall column", 24, 53, false},
		{"gebco grid", 86400, 43200, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			indexer := NewProjectionlessIndexer(tc.width, tc.height, tc.rowMajor)
			checkInd(t, indexer, GridLocation{0, 0}, 0)
			checkInd(t, indexer, GridLocation{tc.width - 1, tc.height - 1}, tc.width*tc.height-1)
			if tc.rowMajor {
				checkInd(t, indexer, GridLocation{1, 0}, 1)
				checkInd(t, indexer, GridLocation{tc.width - 1, 0}, tc.width-1)
				checkInd(t, indexer, GridLocation{0, tc.height - 1}, tc.width*(tc.height-1))
			} else {
				checkInd(t, indexer, GridLocation{0, 1}, 1)
				checkInd(t, indexer, GridLocation{0, tc.height - 1}, tc.height-1)
				checkInd(t, indexer, GridLocation{tc.width - 1, 0}, (tc.width-1)*tc.height)
			}
		})
	}

	indexer := NewProjectionlessIndexer(10, 10, true)
	for i := 0; i < indexer.Size(); i++ {
		x := i % 10
		y := i / 10
		ind, err := indexer.ToIndex(GridLocation{X: x, Y: y})
		if err != nil {
			t.Fatal(err)
		}
		if ind != i {
			t.Errorf("expected to see index %d at %d,%d, but got %d", i, x, y, ind)
		}
	}
}

func TestMercatorCutoffIndexer(t *testing.T) {
	testCases := []struct {
		name        string
		cutoffNorth float64
		cutoffSouth float64
		width       int
		height      int
	}{
		{"square 80/80", 80 * math.Pi / 180, -80 * math.Pi / 180, 100, 100},
		{"rect 60/56", 60 * math.Pi / 180, -56 * math.Pi / 180, 100, 50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			indexer := NewMercatorCutoffIndexer(tc.cutoffNorth, tc.cutoffSouth, tc.width, tc.height, true)
			checkOutOfBounds(t, indexer, SphericalLocation{math.Pi / 2, 0})
			checkOutOfBounds(t, indexer, SphericalLocation{-math.Pi / 2, 0})
			checkInd(t, indexer, SphericalLocation{tc.cutoffSouth, -math.Pi}, 0)
			checkInd(t, indexer, SphericalLocation{tc.cutoffSouth, math.Pi}, tc.width-1)
			checkInd(t, indexer, SphericalLocation{tc.cutoffNorth, -math.Pi}, tc.width*(tc.height-1))
			checkInd(t, indexer, SphericalLocation{tc.cutoffNorth, math.Pi}, tc.width*tc.height-1)
		})
	}
}

func TestCylindricalEquirectangularIndexer(t *testing.T) {
	testCases := []struct {
		name     string
		parallel float64
		width    int
		height   int
	}{
		{"tiny square 0", 0, 3, 3},
		{"tiny width 0", 0, 3, 101},
		{"tiny height 0", 0, 101, 3},
		{"square 0", 0, 100, 100},
		{"rect wide 0", 0, 100, 50},
		{"rect tall 0", 0, 50, 100},
		{"huge square 0", 0, 100_000, 100_000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			indexer := NewCylindricalEquirectangularIndexer(tc.parallel, tc.width, tc.height, true)
			checkInd(t, indexer, SphericalLocation{-math.Pi / 2, -math.Pi}, 0)
			checkInd(t, indexer, SphericalLocation{-math.Pi / 2, math.Pi}, tc.width-1)
			checkInd(t, indexer, SphericalLocation{math.Pi / 2, -math.Pi}, tc.width*(tc.height-1))
			checkInd(t, indexer, SphericalLocation{math.Pi / 2, math.Pi}, tc.width*tc.height-1)
			checkInd(t, indexer, SphericalLocation{0, 0}, (tc.width*((tc.height-1)/2))+(tc.width-1)/2)
		})
	}
}

func checkOutOfBounds(t *testing.T, indexer LocationIndexer, loc Location) {
	_, err := indexer.ToIndex(loc)
	var locErr LocationOutOfBoundsError
	if err == nil || !errors.As(err, &locErr) {
		t.Errorf("expected out of bounds error, got %v", err)
	}
}

func checkInd(t *testing.T, indexer LocationIndexer, loc Location, expected int) {
	ind, err := indexer.ToIndex(loc)
	if err != nil {
		t.Error(err)
	} else if ind != expected {
		t.Errorf("expected index %d for x,y = %v, got %d", expected, loc, ind)
	}
}
