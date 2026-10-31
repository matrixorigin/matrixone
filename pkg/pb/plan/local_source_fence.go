// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

// RequiresLocalSourceFence identifies the existing local source-fence triggers.
// A triggered fence must still validate the complete remote feature set.
func (features RemoteExpressionFeatures) RequiresLocalSourceFence() bool {
	return features.ODKUStatementResult || features.SessionLastInsertIDBaseline || features.JSONMinMaxTypedComparison || features.JSONStringConsumerOverload || features.JSONOpaqueTextContracts
}

func (features *RemoteExpressionFeatures) addLocalSourceFenceExpr(current *Expr) {
	fn := current.GetF()
	if fn == nil || fn.Func == nil {
		return
	}
	id, overload := int32(fn.Func.Obj>>32), int32(fn.Func.Obj)
	features.JSONMinMaxTypedComparison = features.JSONMinMaxTypedComparison || IsJSONMinMaxAggregate(fn.Func.Obj, fn.Args)
	// LAST_INSERT_ID needs the coordinator session baseline.
	features.SessionLastInsertIDBaseline = features.SessionLastInsertIDBaseline || (id == 237 && overload == 0)
	features.JSONStringConsumerOverload = features.JSONStringConsumerOverload || isRemoteJSONStringConsumerOverload(id, overload)
	features.JSONOpaqueTextContracts = features.JSONOpaqueTextContracts || isJSONOpaqueTextContract(current, fn, id)
}

func (features *RemoteExpressionFeatures) addSpecialIntegerConsumer(fn *Function, id, overload int32) error {
	special, err := isSpecialIntegerConsumer(fn, id, overload)
	if err == nil {
		features.SpecialIntegerConsumers = features.SpecialIntegerConsumers || special
	}
	return err
}

func (features *RemoteExpressionFeatures) addPrivateIntegerCast(current *Expr, id, overload int32) error {
	if id == 21 && overload >= 5 && overload <= 8 {
		if err := validateIntegerArgumentCast(current, overload); err != nil {
			return err
		}
		features.IntegerParameterCoercion = true
	}
	return nil
}

func (features *RemoteExpressionFeatures) addNumericFormat(fn *Function) error {
	format, err := isNumericFormatFunction(fn)
	if err == nil && format {
		features.FormatNumericArguments = true
	}
	return err
}

// MayRequireLocalSourceFence proves only the negative case. It preserves the
// complete owner's traversal and collection errors, but skips classifications
// that the local fence never consumes. No observation survives this call.
func MayRequireLocalSourceFence(owner any) (bool, error) {
	var features RemoteExpressionFeatures
	metadata := false
	err := walkExpressionsInOwner(owner, func(expr *Expr) error {
		return VisitExprTree(expr, func(current *Expr) error {
			features.addLocalSourceFenceExpr(current)
			fn := current.GetF()
			if fn != nil && fn.Func != nil {
				id, overload := int32(fn.Func.Obj>>32), int32(fn.Func.Obj)
				if err := features.addSpecialIntegerConsumer(fn, id, overload); err != nil {
					return err
				}
				if err := features.addPrivateIntegerCast(current, id, overload); err != nil {
					return err
				}
			}
			return features.addNumericFormat(fn)
		})
	}, func(owner any) {
		switch owner.(type) {
		case interface{ GetTrackOdkuResult() bool }, interface{ GetOdkuResultTracking() bool }, *DedupJoinCtx:
			// The full collector is the sole getter/metadata authority, including
			// its ordering relative to a later malformed expression.
			metadata = true
		}
	})
	if metadata {
		return true, nil
	}
	return features.RequiresLocalSourceFence(), err
}
