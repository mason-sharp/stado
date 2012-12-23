###############################################################################
 # Copyright (C) 2008 EnterpriseDB Corporation.
 # Copyright (C) 2011 Stado Global Development Group.
 #
 # This program is free software; you can redistribute it and/or modify it
 # under the terms of the GNU General Public License version 2 as published by
 # the Free Software Foundation.
 #
 # This program is distributed in the hope that it will be useful, but WITHOUT
 # ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 # FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 # more details.
 #
 # You should have received a copy of the GNU General Public License along
 # with this program; if not, see http://www.gnu.org/licenses or write to the
 # Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 # 02110-1301 USA.
 #
 # You can find Stado at http://www.stado.us
 #
 ##############################################################################

generate:./sql_extendb.jj
	rm -rf jtb.out.jj
	mv syntaxtree/skip_to_matching_brace.java syntaxtree/skip_to_matching_brace.notremove
	rm -rf *.java
	rm -rf syntaxtree/*.java
	mv syntaxtree/skip_to_matching_brace.notremove syntaxtree/skip_to_matching_brace.java
	rm -rf visitor/*.java
	java -classpath ../../../../../../lib/jtb.jar EDU.purdue.jtb.JTB -p org.postgresql.stado.parser.core ./sql_extendb.jj
	java -classpath ../../../../../../lib/javacc.jar -Xms512M -Xmx512M javacc jtb.out.jj

clean:
	rm -rf *.class
	

