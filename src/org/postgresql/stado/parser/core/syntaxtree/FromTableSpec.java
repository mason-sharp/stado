//
// Generated by JTB 1.2.2
//

package org.postgresql.stado.parser.core.syntaxtree;

/**
 * Grammar production:
 * f0 -> TableSpec(prn)
 * f1 -> ( <CROSS_> <JOIN_> TableSpec(prn) | ( [ <INNER_> ] <JOIN_> TableSpec(prn) JoinSpec(prn) | ( <LEFT_> | <RIGHT_> | <FULL_> ) [ <OUTER_> ] <JOIN_> TableSpec(prn) JoinSpec(prn) ) | <NATURAL_> ( [ <INNER_> ] <JOIN_> TableSpec(prn) | ( <LEFT_> | <RIGHT_> | <FULL_> ) [ <OUTER_> ] <JOIN_> TableSpec(prn) ) )*
 */
public class FromTableSpec implements Node {
   public TableSpec f0;
   public NodeListOptional f1;

   public FromTableSpec(TableSpec n0, NodeListOptional n1) {
      f0 = n0;
      f1 = n1;
   }

   public void accept(org.postgresql.stado.parser.core.visitor.Visitor v) {
      v.visit(this);
   }
   public Object accept(org.postgresql.stado.parser.core.visitor.ObjectVisitor v, Object argu) {
      return v.visit(this,argu);
   }
}
