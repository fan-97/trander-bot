package indicator

import "trader-bot/internal/market"

// Volume returns the latest closed bar's volume.
type Volume struct{ series }

func NewVolume() *Volume { return &Volume{} }

func (v *Volume) Update(k market.Kline) {
	if k.Closed {
		v.set(k.Volume)
	}
}

// VolumeRatio is the current volume divided by the mean of the previous n
// volumes (current excluded); needs n+1 closed bars. Undefined if that mean is 0.
type VolumeRatio struct {
	series
	n    int
	ring *Ring
}

func NewVolumeRatio(n int) *VolumeRatio { return &VolumeRatio{n: n, ring: NewRing(n + 1)} }

func (v *VolumeRatio) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	v.ring.Push(k.Volume)
	if !v.ring.Full() {
		return
	}
	var sum float64
	for i := 0; i < v.n; i++ {
		sum += v.ring.At(i)
	}
	if mean := sum / float64(v.n); mean != 0 {
		v.set(k.Volume / mean)
	}
}
