//
// Generated by JTB 1.2.2
//

package org.postgresql.stado.parser.core.syntaxtree;

/**
 * Grammar production:
 * f0 -> ( Identifier(prn) | <TEMPDOT_> Identifier(prn) | <PUBLICDOT_> Identifier(prn) | <QPUBLICDOT_> Identifier(prn) )
 */
public class TableName implements Node {
   public NodeChoice f0;

   public TableName(NodeChoice n0) {
      f0 = n0;
   }

   public void accept(org.postgresql.stado.parser.core.visitor.Visitor v) {
      v.visit(this);
   }
   public Object accept(org.postgresql.stado.parser.core.visitor.ObjectVisitor v, Object argu) {
      return v.visit(this,argu);
   }
}
