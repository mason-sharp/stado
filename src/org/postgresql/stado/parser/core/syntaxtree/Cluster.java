//
// Generated by JTB 1.2.2
//

package org.postgresql.stado.parser.core.syntaxtree;

/**
 * Grammar production:
 * f0 -> <CLUSTER_>
 * f1 -> [ Identifier(prn) [ <ON_> Identifier(prn) ] ]
 */
public class Cluster implements Node {
   public NodeToken f0;
   public NodeOptional f1;

   public Cluster(NodeToken n0, NodeOptional n1) {
      f0 = n0;
      f1 = n1;
   }

   public Cluster(NodeOptional n0) {
      f0 = new NodeToken("CLUSTER");
      f1 = n0;
   }

   public void accept(org.postgresql.stado.parser.core.visitor.Visitor v) {
      v.visit(this);
   }
   public Object accept(org.postgresql.stado.parser.core.visitor.ObjectVisitor v, Object argu) {
      return v.visit(this,argu);
   }
}
