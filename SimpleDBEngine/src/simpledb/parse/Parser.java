package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.AvgFn;
import simpledb.materialize.CountFn;
import simpledb.materialize.MaxFn;
import simpledb.materialize.MinFn;
import simpledb.materialize.SumFn;
import simpledb.query.*;
import simpledb.record.*;

/**
 * The SimpleDB parser.
 * @author Edward Sciore
 */
public class Parser {
   private Lexer lex;
   private boolean distinct;
   
   public Parser(String s) {
      lex = new Lexer(s);
   }
   
// Methods for parsing predicates, terms, expressions, constants, and fields
   
   public String field() {
	   
	  if (lex.matchId())
		return lex.eatId();
	  
	  else {
		  String aggfn = "";
		  List<String> aggfuncs = Arrays.asList("count", "max", "min", "avg", "sum");
		  for (String f: aggfuncs) {
			  if (lex.matchKeyword(f)) {
				  aggfn += f;
				  aggfn += "(";
				  
				  lex.eatKeyword(f);
				  lex.eatDelim('(');
				  
				  if (lex.matchKeyword("distinct")) {
					  this.distinct = true;
					  lex.eatKeyword("distinct");
				  }
				  
				  String fieldname = lex.eatId();
				  aggfn += fieldname;
				  aggfn += ")";
				  lex.eatDelim(')');
				  
				  System.out.println(aggfn);
				  return aggfn;
			  }
		  }
		  // Throws Error
		  return lex.eatId();
	  }
   }
   
   public AggregationFn aggFn(String f) {
	   List<String> aggfuncs = Arrays.asList("count", "max", "min", "avg", "sum");
	   String parsed[] = f.split("\\(");
	   for (String fn : aggfuncs) {
		   if (parsed[0].equals(fn)) {
			  AggregationFn func = null;
			  
			  String fieldname = parsed[1].split("\\)")[0];
			  System.out.println("Field: " + fieldname);
			  
			  if (fn.equals("count")) {
				  func = new CountFn(fieldname);
			  }
			  
			  else if (fn.equals("max")) {
				  func = new MaxFn(fieldname);
			  }
			  
			  else if (fn.equals("min")) {
				  func = new MinFn(fieldname);
			  }
			  
			  else if (fn.equals("avg")) {
				  func = new AvgFn(fieldname);
			  }
			  
			  else if (fn.equals("sum")) {
				  func = new SumFn(fieldname);
			  }
			  
			  return func;
		   }
	   }
	   return null;
   }
   
   public Constant constant() {
      if (lex.matchStringConstant())
         return new Constant(lex.eatStringConstant());
      else
         return new Constant(lex.eatIntConstant());
   }
   
   public Expression expression() {
      if (lex.matchId())
         return new Expression(field());
      else
         return new Expression(constant());
   }
   
   public Term term() {
      Expression lhs = expression();
      String op = lex.eatOperator();
      Expression rhs = expression();
      return new Term(lhs, rhs, op);
   }
   
   public Predicate predicate() {
      Predicate pred = new Predicate(term());
      if (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         pred.conjoinWith(predicate());
      }
      return pred;
   }
   
// Methods for parsing queries
   
   public QueryData query() {
	  this.distinct = false;
      lex.eatKeyword("select");
      
      boolean distinct = false;
      if (lex.matchKeyword("distinct")) {
    	  lex.eatKeyword("distinct");
    	  this.distinct = true;
      }
      List<String> fields = selectList();
      
      
      // Get AggFns
      List<AggregationFn> aggfn = new ArrayList<AggregationFn>();
      for (int i=0; i<fields.size(); ++i) {
    	  if (!fields.get(i).contains("(") || !fields.get(i).contains(")"))
    		  continue;
    	  AggregationFn agfn = aggFn(fields.get(i));
		  String parsedField = fields.get(i).split("\\(")[1].split("\\)")[0];
		  fields.set(i, parsedField);
    	  if (agfn != null) {
    		  aggfn.add(agfn);
    	  }
      }
      
      System.out.println(fields.get(0));
      
      lex.eatKeyword("from");
      Collection<String> tables = tableList();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      
      List<String> groupByFields = new ArrayList<String>();
      
      // Group By
      if (lex.matchKeyword("group")) {
    	  lex.eatKeyword("group");
    	  if (lex.matchKeyword("by")) {
    		  lex.eatKeyword("by");
    		  String field;
    		  while (lex.matchId()) {
    			  field = lex.eatId();
    			  groupByFields.add(field);
    			  if (lex.matchDelim(','))
    				  lex.eatDelim(',');
    			  else break;  
    		  }
    	  }
      }
      
      List<String> sortFields = new ArrayList<String>();
      List<String> sortOrder = new ArrayList<String>();
      // Order By
      if (lex.matchKeyword("order")) {
    	  lex.eatKeyword("order");
    	  if (lex.matchKeyword("by")) {
    		  lex.eatKeyword("by");
    		  String field;
    		  String order;
    		  while (lex.matchId()) {
    			  field = lex.eatId();
    			  
    			  if (lex.matchKeyword("asc")) {
    				  order = "asc";
    				  lex.eatKeyword("asc");
    			  }
    			  
    			  else if (lex.matchKeyword("desc")) {
    				  order = "desc";
    				  lex.eatKeyword("desc");
    			  }
    			  else {
    				  order = "asc";
    			  }
    			  
    			  sortFields.add(field);
    			  sortOrder.add(order);
    			  
    			  if (lex.matchDelim(','))
    				  lex.eatDelim(',');
    			  
    			  else break;  
    		  }
    	  }
      }
      
      System.out.println("End of parser");
      return new QueryData(fields, tables, pred, sortFields, sortOrder, groupByFields, aggfn, this.distinct);
   }
   
   private List<String> selectList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(selectList());
      }
      return L;
   }
   
   private Collection<String> tableList() {
      Collection<String> L = new ArrayList<String>();
      L.add(lex.eatId());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(tableList());
      }
      return L;
   }
   
// Methods for parsing the various update commands
   
   public Object updateCmd() {
      if (lex.matchKeyword("insert"))
         return insert();
      else if (lex.matchKeyword("delete"))
         return delete();
      else if (lex.matchKeyword("update"))
         return modify();
      else
         return create();
   }
   
   private Object create() {
      lex.eatKeyword("create");
      if (lex.matchKeyword("table"))
         return createTable();
      else if (lex.matchKeyword("view"))
         return createView();
      else
         return createIndex();
   }
   
// Method for parsing delete commands
   
   public DeleteData delete() {
      lex.eatKeyword("delete");
      lex.eatKeyword("from");
      String tblname = lex.eatId();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new DeleteData(tblname, pred);
   }
   
// Methods for parsing insert commands
   
   public InsertData insert() {
      lex.eatKeyword("insert");
      lex.eatKeyword("into");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      List<String> flds = fieldList();
      lex.eatDelim(')');
      lex.eatKeyword("values");
      lex.eatDelim('(');
      List<Constant> vals = constList();
      lex.eatDelim(')');
      return new InsertData(tblname, flds, vals);
   }
   
   private List<String> fieldList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(fieldList());
      }
      return L;
   }
   
   private List<Constant> constList() {
      List<Constant> L = new ArrayList<Constant>();
      L.add(constant());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(constList());
      }
      return L;
   }
   
// Method for parsing modify commands
   
   public ModifyData modify() {
      lex.eatKeyword("update");
      String tblname = lex.eatId();
      lex.eatKeyword("set");
      String fldname = field();
      lex.eatDelim('=');
      Expression newval = expression();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new ModifyData(tblname, fldname, newval, pred);
   }
   
// Method for parsing create table commands
   
   public CreateTableData createTable() {
      lex.eatKeyword("table");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      Schema sch = fieldDefs();
      lex.eatDelim(')');
      return new CreateTableData(tblname, sch);
   }
   
   private Schema fieldDefs() {
      Schema schema = fieldDef();
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         Schema schema2 = fieldDefs();
         schema.addAll(schema2);
      }
      return schema;
   }
   
   private Schema fieldDef() {
      String fldname = field();
      return fieldType(fldname);
   }
   
   private Schema fieldType(String fldname) {
      Schema schema = new Schema();
      if (lex.matchKeyword("int")) {
         lex.eatKeyword("int");
         schema.addIntField(fldname);
      }
      else {
         lex.eatKeyword("varchar");
         lex.eatDelim('(');
         int strLen = lex.eatIntConstant();
         lex.eatDelim(')');
         schema.addStringField(fldname, strLen);
      }
      return schema;
   }
   
// Method for parsing create view commands
   
   public CreateViewData createView() {
      lex.eatKeyword("view");
      String viewname = lex.eatId();
      lex.eatKeyword("as");
      QueryData qd = query();
      return new CreateViewData(viewname, qd);
   }
   
   
//  Method for parsing create index commands
   
   public CreateIndexData createIndex() {
      lex.eatKeyword("index");
      String idxname = lex.eatId();
      lex.eatKeyword("on");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      String fldname = field();
      lex.eatDelim(')');
      
      lex.eatKeyword("using");
      
      String indexType;
      
      if (lex.matchKeyword("btree")) {
    	  indexType = "btree";
    	  lex.eatKeyword("btree");
      }
      
      else if (lex.matchKeyword("hash")) {
    	  indexType = "hash";
    	  lex.eatKeyword("hash");
      }
      
      else {
    	  throw new BadSyntaxException();
      }
      
      return new CreateIndexData(idxname, tblname, fldname, indexType);
   }
}

