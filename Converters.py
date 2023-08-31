import codecs
import os
import pandas as pd

class Converters():
    def __init__(self):
    #Semantic context
        self.COL_DELIMITER = ";"
        self.ATTR_DELIMITER = ":"
        self.TABLE_NAME_HEADER = "* Table: "
        self.COL_DEF_HEADER = "$"
        self.NEWLINE_WIN = "\r\n"
        self.NEWLINE_MAC = "\n"
        self.NEWLINES = [self.NEWLINE_WIN, self.NEWLINE_MAC]
        self.NEWLINE_DET = None
        self.HEADER = None

        #table flags and variables
        self._table_flag = False
        self._line_count = 0
        self._cols = None
        self._table_names = None
        self._col_defs = None
        
        return None

    def net2csv(self,
                path: str,
                export_path: str=None
               ) -> None:
        """
        This converter parse the editable PTV Visum Net file.
        It parses line by line and listens for table header, column definitions, data and table end.
        It stores the data to csvs (one csv per Visum table)
        :param path: Path of the editable file (.net) to process
        :param export_path: Path to folder, where to save the CSV files.
                                If not specified, the CSV files are save in
                                folder of the net files.
        :return: None
        """

        # use path of net-file for export if no export_path is specified:
        if export_path is None:
            export_path = os.path.dirname(path)
        self.export_path = export_path
        _col_identified_flag = True
        _header_flag = False
        ix_header = 0
        with open(path, encoding='utf-8', errors='ignore') as net:
            for line in net:
                #find header of net file
                if '$VISION' in line:
                    _header_flag = True
                if _header_flag and ix_header <=2:
                    ix_header +=1
                    if self.HEADER == None: self.HEADER = []
                    self.HEADER.append(line)
                # Autodetect COL_DELIMITER:
                if _col_identified_flag:
                    if "$VERSION:" in line:
                        tmp = line.replace("$VERSION:", "").strip()
                        for delimiter in [" ", ";", "\t"]:
                            if delimiter in tmp:
                                self.COL_DELIMITER = delimiter
                                print('Use delimiter "%s"' % self.COL_DELIMITER)
                                _col_identified_flag = False

                if line.startswith(self.TABLE_NAME_HEADER):
                    # new table
                    _table_name = line.split(self.ATTR_DELIMITER)[-1].replace(" ", "").strip()
                    if self._table_names == None: self._table_names = []
                    self._table_names.append(line)
                    print("Parsing table: ", _table_name)
                    self._line_count = 0
                if line[0] == self.COL_DEF_HEADER:
                    # line with column names
                    self._table_flag = True
                    data = list()  # initialize data
                    line = line.replace(self.NEWLINE_WIN, "").replace("\r", "")
                    if len(line.split(":")) > 1:
                        self._cols = line.split(":")[1].split(self.COL_DELIMITER)
                        if self._col_defs == None: self._col_defs = []
                        self._col_defs.append(line.split(':')[0][1:])
                    else:
                        self._cols = line.split(self.COL_DELIMITER)
                if line in self.NEWLINES:
                    if self.NEWLINE_DET == None:
                        self.NEWLINE_DET = line
                    self._table_flag = False
                    if len(data) > 0:
                        # only if table is not empty
                        if self._cols is not None:
                            _file_name = os.path.join(export_path, _table_name + '.csv')
                            pd.DataFrame(data, columns=self._cols).to_csv(_file_name, index=False, float_format="%.3f")
                            print("Exported {} objects in table: {} - saved to {}"
                                .format(self._line_count, _table_name, _file_name))
                            data = list()  # initialize data
                            self._line_count = 0

                if self._table_flag and line[0] != self.COL_DEF_HEADER:
                    data.append(line.strip().split(self.COL_DELIMITER))
                    self._line_count += 1
        net.close()
        return None
    
    def __setPrec(self,value):
        """
        Map function to set precision of floats.
        """
        if isinstance(value, float): return "{:.3f}".format(value)
        else: return str(value)
    
    def csv2net(self,
                csv_path: str = None,
                net_path: str=None
               ) -> None:
        """
        This converter create a net file with csv of the Network objects.
       
        :param csv_path: Path to folder, where to find the CSV files.
                                If not specified, the CSV files are searched in
                                folder of the exported csv files in the net2csv method.

        :param net_path: Path of the output editable file (.net)

        :return: None
        """
        if csv_path == None:
            csv_path = self.export_path
        if net_path == None:
            net_path = 'processed.net'
        print(net_path)
        
        with open(net_path, mode='w', encoding='utf-8', errors='ignore') as net:
            #write header
            net.writelines(self.HEADER)
            #write tables
            for tab, col_def in zip(self._table_names, self._col_defs):
                #table header
                net.writelines(['* ' +self.NEWLINE_DET,
                                tab,
                                '* ' +self.NEWLINE_DET])
                #columns header
                tab_file = os.path.join(csv_path, tab.split(self.ATTR_DELIMITER)[-1].replace(" ", "").strip() + '.csv')
                df = pd.read_csv(tab_file)
                #remove nan and format decimal places
                df = df.fillna('')
                net.writelines('$' + col_def + ':' + ';'.join(map(str, df.columns.values.tolist())))
                #row data
                net.writelines([';'.join(map(self.__setPrec, l)) + self.NEWLINE_DET for l in df.values.tolist()])
                #newline to split tables
                net.write(self.NEWLINE_DET)
        net.close()
        return None


