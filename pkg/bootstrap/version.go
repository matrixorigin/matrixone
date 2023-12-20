package bootstrap

func (v Version) NeedUpgrade() bool {
	return v.upgrade == 1
}

func (v Version) NeedUpgradeTenant() bool {
	return v.upgradeTenant == 1
}

func (v Version) CanUpgradeToCurrent(version string) bool {
	return v.prevVersion == version
}
