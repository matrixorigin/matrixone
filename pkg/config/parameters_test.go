package config

import (
	"testing"
)

///*
type AllParameters struct {

}

func (ap *AllParameters) LoadInitialValues()error {
	return nil
}

func (ap *AllParameters) UpdateParametersWithConfiguration(*configuration)error{
	return nil
}

type configuration struct{

}

func (config *configuration) LoadConfigurationFromFile(string) error{
	return nil
}

func (config *configuration) LoadConfigurationFromString(input string) error {
	return nil
}
//*/

/**
You should execute Test_ParameterDefinitionAndTemplate2 first
 */

func TestAllParameters_LoadInitialValues(t *testing.T) {
	ap := &AllParameters{}
	if err :=ap.LoadInitialValues(); err!=nil{
		t.Errorf("LoadInitialValues failed. error:%v",err)
	}
}

func Test_configuration_LoadConfigurationFromString(t *testing.T) {
	t1:=`
		boolSet1 = true
		boolSet2 = false
		boolSet3 = true
		stringSet1 = "ss1"
		stringSet2 = "xxx"
		int64set1 = 5
		int64set3 = 10
		int64Range1 = 1001
		float64set1 = 4.0
		float64set3 = 0.5
		float64Range1 = 900.0
	`

	t2:=`
		boolSet1 = false
	`

	t3:=`
		boolSet2 = true
	`

	t4:=`
		boolSet3 = false
	`

	t5:=`
		stringSet1 = "ss4"
	`

	t6:=`
		stringSet2 = "ppp"
	`

	t7:=`int64set1 = 7`
	t8:=`int64set3 = -1`
	t9:=`int64Range1 = -1`
	t10:=`int64Range1 = 10005`
	t11:=`float64set1 = 7.0`
	t12:=`float64set3 = 0.001`
	t13:=`float64Range1 = 0.0`
	t14:=`float64Range1 = 10005.03`
	t15:=`float64set1 = 7`

	type args struct {
		input string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		wantErr2 bool
	}{
		{"t1",args{t1},false,false},
		{"t2",args{t2},false,true},
		{"t3",args{t3},false,true},
		{"t4",args{t4},false,false},
		{"t5",args{t5},false,true},
		{"t6",args{t6},false,false},
		{"t7",args{t7},false,true},
		{"t8",args{t8},false,false},
		{"t9",args{t9},false,true},
		{"t10",args{t10},false,true},
		{"t11",args{t11},false,true},
		{"t12",args{t12},false,false},
		{"t13",args{t13},false,true},
		{"t14",args{t14},false,true},
		{"t15",args{t15},true,true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ap := &AllParameters{}
			if err := ap.LoadInitialValues(); err != nil{
				t.Errorf("LoadInitialValues failed.error %v",err)
			}
			config := &configuration{}
			if err := config.LoadConfigurationFromString(tt.args.input); (err != nil) != tt.wantErr {
				t.Errorf("LoadConfigurationFromString() error = %v, wantErr %v", err, tt.wantErr)
			}else if err != nil{
				return
			}

			if err := ap.UpdateParametersWithConfiguration(config); (err != nil) != tt.wantErr2{
				t.Errorf("UpdateParametersWithConfiguration failed. error:%v",err)
			}
		})
	}
}

func Test_configuration_LoadConfigurationFromFile(t *testing.T) {
	type args struct {
		input string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		wantErr2 bool
	}{
		{"t1",args{"config.toml"},false,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ap := &AllParameters{}
			if err := ap.LoadInitialValues(); err != nil{
				t.Errorf("LoadInitialValues failed.error %v",err)
			}
			config := &configuration{}
			if err := config.LoadConfigurationFromFile(tt.args.input); (err != nil) != tt.wantErr {
				t.Errorf("LoadConfigurationFromFile error = %v, wantErr %v", err, tt.wantErr)
			}else if err != nil{
				return
			}

			if err := ap.UpdateParametersWithConfiguration(config); (err != nil) != tt.wantErr2{
				t.Errorf("UpdateParametersWithConfiguration failed. error:%v",err)
			}
		})
	}
}