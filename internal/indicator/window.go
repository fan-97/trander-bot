package indicator

// Ring is a fixed-capacity ring buffer of float64 values.
type Ring struct {
	buf   []float64
	start int
	n     int
}

// NewRing returns a ring holding at most capacity values.
func NewRing(capacity int) *Ring { return &Ring{buf: make([]float64, capacity)} }

// Push appends v, evicting the oldest value when full.
func (r *Ring) Push(v float64) {
	if r.n < len(r.buf) {
		r.buf[(r.start+r.n)%len(r.buf)] = v
		r.n++
		return
	}
	r.buf[r.start] = v
	r.start = (r.start + 1) % len(r.buf)
}

// Len is the number of stored values.
func (r *Ring) Len() int { return r.n }

// Full reports whether the ring holds capacity values.
func (r *Ring) Full() bool { return r.n == len(r.buf) }

// At returns the i-th oldest value (0 = oldest).
func (r *Ring) At(i int) float64 { return r.buf[(r.start+i)%len(r.buf)] }

// Last returns the newest value; the ring must be non-empty.
func (r *Ring) Last() float64 { return r.At(r.n - 1) }
