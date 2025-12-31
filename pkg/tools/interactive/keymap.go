// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package interactive

// KeyAction represents a keyboard action
type KeyAction int

const (
	ActionNone KeyAction = iota
	ActionQuit
	ActionMoveUp
	ActionMoveDown
	ActionMoveLeft
	ActionMoveRight
	ActionPageUp
	ActionPageDown
	ActionHome
	ActionEnd
	ActionEnter
	ActionBack
	ActionEscape
	ActionSearch
	ActionFilter
	ActionHelp
	ActionNextMatch
	ActionPrevMatch
)

// KeyMap maps key strings to actions
type KeyMap map[string]KeyAction

// DefaultKeyMap returns the default key mapping
func DefaultKeyMap() KeyMap {
	return KeyMap{
		"q":         ActionQuit,
		"ctrl+c":    ActionQuit,
		"j":         ActionMoveDown,
		"down":      ActionMoveDown,
		"k":         ActionMoveUp,
		"up":        ActionMoveUp,
		"h":         ActionMoveLeft,
		"left":      ActionMoveLeft,
		"l":         ActionMoveRight,
		"right":     ActionMoveRight,
		"pgdown":    ActionPageDown,
		"ctrl+f":    ActionPageDown,
		"pgup":      ActionPageUp,
		"ctrl+b":    ActionPageUp,
		"g":         ActionHome,
		"G":         ActionEnd,
		"enter":     ActionEnter,
		"b":         ActionBack,
		"backspace": ActionBack,
		"esc":       ActionEscape,
		"/":         ActionSearch,
		"f":         ActionFilter,
		"?":         ActionHelp,
		"n":         ActionNextMatch,
		"N":         ActionPrevMatch,
	}
}

// GetAction returns the action for a key string
func (km KeyMap) GetAction(key string) KeyAction {
	if action, ok := km[key]; ok {
		return action
	}
	return ActionNone
}
