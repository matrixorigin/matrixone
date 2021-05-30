package tree

//Visitor Design Pattern
//Visitor visits the node or sub nodes
type Visitor interface {
	Enter(Expr)(bool,Expr)
	Exit(Expr)(Expr)
}