package aimd

type AIMD struct {
	Min            float64
	Max            float64
	HysteresisBand float64
	PreviousRate   float64
	PreviousOut    float64
}

func (c *AIMD) Init(p ...float64) {
	c.Min = p[0]
	c.Max = p[1]
	c.HysteresisBand = p[2]
	c.PreviousRate = p[3]
	c.PreviousOut = p[4]
}

func (c *AIMD) Update(p ...float64) float64 {
	u := 0.0
	setpoint := p[0]
	y := p[1] // measured arrival rate

	if y < (setpoint - c.HysteresisBand) { // The system is bellow the goal  TODO
		if y > c.PreviousRate {
			u = c.PreviousOut + 1
			//fmt.Printf("Accelerating+ [%.4f][%.4f][%.4f][%.4f]\n", y, setpoint, c.PreviousOut, u)
		} else {
			u = c.PreviousOut * 2
			//fmt.Printf("Reducing-- [%.4f][%.4f][%.4f][%.4f]\n", y, setpoint, c.PreviousOut, u)
		}
		//} else if y > (setpoint + c.Info.HysteresisBand) { // The system is above the goal TODO
	} else if y > (setpoint + c.HysteresisBand) { // The system is above the goal
		if y < c.PreviousRate {
			u = c.PreviousOut - 1
			//fmt.Printf("Reducing- [%.4f][%.4f][%.4f][%.4f]\n", y, setpoint, c.PreviousOut, u)
		} else {
			u = c.PreviousOut / 2
			//fmt.Printf("Accelerating++ [%.4f][%.4f][%.4f][%.4f]\n", y, setpoint, c.PreviousOut, u)
		}
	} else { // The system is at Optimum state, no action required
		u = c.PreviousOut
		//fmt.Printf("Optimum Level \n")
	}

	// final check of rnew
	if u < c.Min {
		u = c.Min
	}
	if u > c.Max {
		u = c.Max
	}

	//fmt.Printf("[Rate=%.4f -> %.4f], [PC=%.4f -> %.4f]\n", c.PreviousRate, y, c.PreviousOut, u)

	c.PreviousOut = u
	c.PreviousRate = y

	return u
}
