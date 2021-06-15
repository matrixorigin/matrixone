package engine

func EncodeTable(defs []TableDef, p *PartitionBy, d *DistributionBy) ([]byte, error) {
	return nil, nil
}

func DecodeTable(_ []byte) ([]TableDef, *PartitionBy, *DistributionBy, error) {
	return nil, nil, nil, nil
}

func EncodeTableDef(def TableDef) ([]byte, error) {
	return nil, nil
}

func DecodeTableDef(_ []byte) (TableDef, error) {
	return nil, nil
}

func EncodePartionBy(p *PartitionBy) ([]byte, error) {
	return nil, nil
}

func DecodePartionBy(p *PartitionBy) ([]byte, error) {
	return nil, nil
}
