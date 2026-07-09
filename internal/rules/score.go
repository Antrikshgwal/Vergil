package rules

type Features struct {
	Velocity  int
	Amount    float64
	AmountSum float64 // running spend total in the current fixed window
	Currency  string
}

type Rule interface {
	Evaluate(f Features) float64 // Evaluate the features and return a score
	Name() string                // Return the name of the feature
}

type HighVelocityRule struct {
	Threshold int
	Point     float64
}

type HighAmountRule struct {
	Threshold float64
	Point     float64
}


type HighAmountSumRule struct {
	Threshold float64
	Point     float64
}

// UnusualCurrencyRule fires when the txn currency is not in the Allowed set.
type UnusualCurrencyRule struct {
	Allowed map[string]bool
	Point   float64
}

func (r HighVelocityRule) Evaluate(f Features) float64 {
	if f.Velocity > r.Threshold {
		return r.Point
	}
	return 0
}

func (r HighAmountRule) Evaluate(f Features) float64 {
	if f.Amount > r.Threshold {
		return r.Point
	}
	return 0
}

func (r HighAmountSumRule) Evaluate(f Features) float64 {
	if f.AmountSum > r.Threshold {
		return r.Point
	}
	return 0
}

func (r UnusualCurrencyRule) Evaluate(f Features) float64 {
	if !r.Allowed[f.Currency] {
		return r.Point
	}
	return 0
}

func (r HighVelocityRule) Name() string {
	return "HighVelocityRule"
}

func (r HighAmountRule) Name() string {
	return "HighAmountRule"
}

func (r HighAmountSumRule) Name() string {
	return "HighAmountSumRule"
}

func (r UnusualCurrencyRule) Name() string {
	return "UnusualCurrencyRule"
}

func ScoreTransaction(f Features, rules []Rule) (float64, []string) {
	var totalScore float64
	triggeredRules := make([]string, 0)

	for _, rule := range rules {
		score := rule.Evaluate(f)
		totalScore += score
		if score > 0 {
			triggeredRules = append(triggeredRules, rule.Name())
		}
	}
	return totalScore, triggeredRules
}

func Classify(score float64) string {
	if score >= 0.7 {
		return "BLOCK"
	} else if score >= 0.4 {
		return "REVIEW"
	}
	return "ALLOW"
}
