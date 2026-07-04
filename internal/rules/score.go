package rules

type Features struct {
	Velocity int
	Amount   float64
	Currency string
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

func (r HighVelocityRule) Name() string {
	return "HighVelocityRule"
}

func (r HighAmountRule) Name() string {
	return "HighAmountRule"
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
