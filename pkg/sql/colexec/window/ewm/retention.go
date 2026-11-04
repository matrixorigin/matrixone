// Copyright 2021 - 2026 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ewm

// genuineTail rebuilds the GENUINE complete batch residual
// (Sum obs - k*mean)/k: the batch sum forms in the xsum4 register
// cascade over the RAW xfrexp(obs) items of the ORIGINAL observations
// (never over already-rounded per-observation quotients, whose
// division residuals an earlier rebuild dropped), the k*mean
// subtrahend enters as its TWO exact xtwoprod words so the
// subtraction residuals chase through the same cascade registers, and
// ONE xdiv at the end keeps the single division rounding.
// genuineTail polls the shared cancellation hook through the
// k-sized observations sweep; a dead poller stops the sweep and the
// partial tail is discarded by the merge layer's dead check.
func genuineTail(obs []float64, k float64, mean float64, poll *poller) xnum {
	items := make([]xnum, 0, len(obs)+2)
	for _, x := range obs {
		if poll.step() != nil {
			return zeroPair
		}
		items = append(items, xfrexp(scalar(x)))
	}
	km, kl := xtwoprod(xfrexp(scalar(k)), xfrexp(scalar(mean)))
	items = append(items, xneg(km), xneg(kl))
	return xdiv(xsum4E(items, poll), xfrexp(scalar(k)))
}

// laneRetains is the ONE shared tail-retention test, applied at every
// commit lane on both streams and both routes and at every
// tail-storage boundary (Invariant 37 / Decision 61, extended to the
// merged-state stores by Invariant 40 / Decision 64). Narrowing the
// complete residual's pair to float64 loses content iff re-normalizing
// the narrowed lead does not reproduce the pair exactly: a NONZERO
// lead quantized to the subnormal lattice, a lead flushed below the
// lattice, or any other lossy narrowing keep the extended pair; zero,
// an exact subnormal word, every normal lead and the honest
// out-of-range ±Inf re-normalize to themselves and keep the narrowed
// scalar lane.
func laneRetains(p xnum) bool {
	v := xval(p)
	if !isFinite(v) {
		return false
	}
	return !xeq(xfrexp(scalar(v)), p)
}

// packRemainder is the ONE shared complete-mean packing procedure at
// every normalization/merge boundary: the FULL extended-minus-narrowed
// difference (the primary xfold result — discarded at these sites
// before rev 20) enters the tail accumulator together with its exact
// fold loss; the caller's existing low word follows. Benign
// exact-narrowing folds append NOTHING and every preserved row stays
// bit-identical.
func packRemainder(leadPair, negNarrowed xnum, items *[]xnum) {
	r, l, lhas := xfold(leadPair, negNarrowed)
	if !xeq(r, zeroPair) {
		*items = append(*items, r)
	}
	if lhas && l.m != 0.0 {
		*items = append(*items, l)
	}
}
