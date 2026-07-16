package process

import (
	"errors"
	"math"
	"sync"
	"testing"
)

func TestHashBuildBudgetExactLimitAndOverflow(t *testing.T) {
	b, err := NewHashBuildBudget(math.MaxUint64, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	g, err := b.OpenGeneration(1)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := g.Reserve(math.MaxUint64)
	if err != nil {
		t.Fatalf("exact limit rejected: %v", err)
	}
	if got := b.AggregateUsed(); got != math.MaxUint64 {
		t.Fatalf("aggregate used = %d, want max uint64", got)
	}
	if _, err = g.Reserve(1); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("limit+1 error = %v, want admission rejection", err)
	}
	if got := b.AggregateUsed(); got != math.MaxUint64 {
		t.Fatalf("failed query reservation changed aggregate: %d", got)
	}
	if got := g.Used(); got != math.MaxUint64 {
		t.Fatalf("failed query reservation changed generation: %d", got)
	}
	if !tok.Release() || tok.Release() {
		t.Fatal("release must transition exactly once")
	}
	if b.AggregateUsed() != 0 || g.Used() != 0 {
		t.Fatalf("released reservation remains: cn=%d query=%d", b.AggregateUsed(), g.Used())
	}
}

func TestHashBuildBudgetQueryRejectRollsBackCN(t *testing.T) {
	b := MustNewHashBuildBudget(10, 4)
	g1, _ := b.OpenGeneration(1)
	g2, _ := b.OpenGeneration(2)
	first, err := g1.Reserve(4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = g2.Reserve(7); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("error = %v, want admission rejection", err)
	}
	if b.AggregateUsed() != 4 || g2.Used() != 0 {
		t.Fatalf("query rejection did not roll back CN: cn=%d g2=%d", b.AggregateUsed(), g2.Used())
	}
	first.Release()
}

func TestHashBuildBudgetConcurrentReserveRelease(t *testing.T) {
	const workers = 32
	b := MustNewHashBuildBudget(workers, workers)
	gens := make([]*HashBuildBudgetGeneration, workers)
	for i := range gens {
		gens[i], _ = b.OpenGeneration(uint64(i + 1))
	}
	start := make(chan struct{})
	acquired := make(chan *HashBuildReservation, workers)
	var wg sync.WaitGroup
	for i := range gens {
		wg.Add(1)
		go func(g *HashBuildBudgetGeneration) {
			defer wg.Done()
			<-start
			tok, err := g.Reserve(1)
			if err == nil {
				acquired <- tok
			}
		}(gens[i])
	}
	close(start)
	wg.Wait()
	if len(acquired) != workers {
		t.Fatalf("acquired %d reservations, want %d", len(acquired), workers)
	}
	if b.AggregateUsed() != workers {
		t.Fatalf("aggregate used = %d, want %d", b.AggregateUsed(), workers)
	}
	for i := 0; i < workers; i++ {
		(<-acquired).Release()
	}
	if b.AggregateUsed() != 0 {
		t.Fatalf("aggregate used after release = %d", b.AggregateUsed())
	}
}

func TestHashBuildBudgetTransferAndClose(t *testing.T) {
	b := MustNewHashBuildBudget(8, 8)
	g, _ := b.OpenGeneration(7)
	tok, err := g.Reserve(3)
	if err != nil {
		t.Fatal(err)
	}
	moved := tok.Transfer()
	if moved == nil || tok.Release() {
		t.Fatal("transfer must make original token inert")
	}
	if b.AggregateUsed() != 3 {
		t.Fatalf("transfer changed charge: %d", b.AggregateUsed())
	}
	g.Close()
	if _, err = g.Reserve(1); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed generation error = %v", err)
	}
	if !moved.Release() || moved.Release() {
		t.Fatal("transferred token release must be exactly once")
	}
	if b.AggregateUsed() != 0 {
		t.Fatalf("live token release after close leaked: %d", b.AggregateUsed())
	}
	b.Close()
	if _, err = b.OpenGeneration(8); !errors.Is(err, ErrHashBuildBudgetClosed) {
		t.Fatalf("closed budget error = %v", err)
	}
}

func TestHashBuildBudgetGenerationIsolation(t *testing.T) {
	b := MustNewHashBuildBudget(8, 8)
	old, _ := b.OpenGeneration(1)
	oldToken, _ := old.Reserve(6)
	old.Close()
	newGeneration, _ := b.OpenGeneration(2)
	newToken, err := newGeneration.Reserve(2)
	if err != nil {
		t.Fatal(err)
	}
	oldToken.Release()
	if newGeneration.Used() != 2 || b.AggregateUsed() != 2 {
		t.Fatalf("old release affected new generation: new=%d aggregate=%d", newGeneration.Used(), b.AggregateUsed())
	}
	newToken.Release()
}

func TestHashBuildBudgetCapReductionFailsClosedUntilRelease(t *testing.T) {
	b := MustNewHashBuildBudget(10, 10)
	g1, _ := b.OpenGenerationWithCap(1, 10)
	owned, err := g1.Reserve(8)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.UpdateAggregateCap(6); err != nil {
		t.Fatal(err)
	}
	g2, _ := b.OpenGenerationWithCap(2, 6)
	if _, err = g2.Reserve(1); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("cap shrink did not fail closed: %v", err)
	}
	owned.Release()
	newToken, err := g2.Reserve(6)
	if err != nil {
		t.Fatalf("reservation after release failed: %v", err)
	}
	newToken.Release()
}

func TestHashBuildBudgetLiveCapProviderShrinksOpenGeneration(t *testing.T) {
	b := MustNewHashBuildBudget(10, 10)
	g, _ := b.OpenGenerationWithCap(1, 10)
	cap := uint64(10)
	b.SetAggregateCapProvider(func() (uint64, error) { return cap, nil })
	owned, err := g.Reserve(6)
	if err != nil {
		t.Fatal(err)
	}
	cap = 5
	if _, err = g.Reserve(1); !errors.Is(err, ErrHashBuildBudgetAdmission) {
		t.Fatalf("open generation ignored live cap shrink: %v", err)
	}
	owned.Release()
	token, err := g.Reserve(5)
	if err != nil {
		t.Fatalf("reservation at refreshed cap failed: %v", err)
	}
	token.Release()
}

func TestResolveHashBuildCeiling(t *testing.T) {
	const gib = uint64(1 << 30)
	got, err := ResolveHashBuildCeiling(HashBuildCeilingInputs{
		CgroupMemoryMax:       20 * gib,
		HostMemTotal:          10 * gib,
		GlobalMpoolCap:        30 * gib,
		FileCacheHint:         gib,
		ProcessLimitationSize: 2 * gib,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.EffectiveCN != 10*gib || got.RequestedReserve != 4*gib || got.Reserve != 4*gib || got.CNHashCap != 6*gib || got.QueryCap != 2*gib {
		t.Fatalf("unexpected ceiling: %+v", got)
	}
	if _, err = ResolveHashBuildCeiling(HashBuildCeilingInputs{CgroupMemoryMax: math.MaxUint64, HostMemTotal: 0, GlobalMpoolCap: 0}); !errors.Is(err, ErrHashBuildCeilingMissing) {
		t.Fatalf("missing finite source error = %v", err)
	}
	small, err := ResolveHashBuildCeiling(HashBuildCeilingInputs{HostMemTotal: 3 * gib, FileCacheHint: 3 * gib})
	if err != nil || small.CNHashCap != 3*gib/20 {
		t.Fatalf("small-CN bounded allowance = %+v, err=%v", small, err)
	}
}
