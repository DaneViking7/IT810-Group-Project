import database_operations

#  This program is used to issue queries to an existing MySQL database
#  MySql server connection information is contained within the "database_operations" Module
#  
#  Queries are stored within the "func_load_default_select_queries()" method of the database_operations module


def main():   
    ##create the databse and database table
    db_interface=database_operations.DB_Parser()
    db_interface.func_load_default_select_queries()
    db_interface.func_run_select_queries()
    db_interface.connection.close()
    db_interface.cursor.close()
    print('Program complete')

if __name__ == "__main__":    
        main()
        
