using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Threading;
using System.Windows;
using System.Windows.Threading;

using GisSharpBlog.NetTopologySuite.Geometries;
using GisSharpBlog.NetTopologySuite.IO;

namespace Shp2Sql
{
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            this.InitializeComponent();
        }

        private SqlCommand CriarComandoInclusao(SqlConnection sqlConnection, string file, ShapefileDataReader shapefileDataReader)
        {
            string nomeTabela = this.RetornarNomeTabela(file);

            StringBuilder sbInsert = new StringBuilder();
            sbInsert.Append(String.Format("INSERT INTO {0} (", nomeTabela));

            for (int index = 0; index < shapefileDataReader.DbaseHeader.NumFields; index++)
                sbInsert.Append(String.Format("       [{0}],", shapefileDataReader.DbaseHeader.Fields[index].Name.ToUpper()));

            sbInsert.Append("       GEO");
            sbInsert.Append(") VALUES (");

            for (int index = 0; index < shapefileDataReader.DbaseHeader.NumFields; index++)
                sbInsert.Append(String.Format("       @{0},", shapefileDataReader.DbaseHeader.Fields[index].Name.ToUpper()));

            sbInsert.Append("       GEOMETRY::STGeomFromWKB(@GEO, 0)");
            sbInsert.Append(")");

            SqlCommand scInsert = new SqlCommand(sbInsert.ToString(), sqlConnection);

            for (int index = 0; index < shapefileDataReader.DbaseHeader.NumFields; index++)
                scInsert.Parameters.Add(new SqlParameter(String.Format("@{0}", shapefileDataReader.DbaseHeader.Fields[index].Name.ToUpper()), SqlDbType.NVarChar));

            scInsert.Parameters.Add(new SqlParameter("@GEO", SqlDbType.Binary));

            return scInsert;
        }

        private void CriarTabela(SqlConnection sqlConnection, string file, ShapefileDataReader shapefileDataReader)
        {
            string nomeTabela = this.RetornarNomeTabela(file);

            // Apagando a tabela caso exista
            StringBuilder sbDrop = new StringBuilder();
            sbDrop.Append(String.Format("IF EXISTS (SELECT 1 FROM SYS.TABLES WHERE object_id = OBJECT_ID('[{0}]'))", nomeTabela.Replace("'", "''")));
            sbDrop.Append(String.Format("DROP TABLE [{0}]", nomeTabela));

            SqlCommand scDrop = new SqlCommand(sbDrop.ToString(), sqlConnection);
            scDrop.ExecuteNonQuery();

            // Criando a tabela
            StringBuilder sbCreate = new StringBuilder();
            sbCreate.Append(String.Format("CREATE TABLE [{0}] (", nomeTabela));
            sbCreate.Append("       ID INT IDENTITY NOT NULL,");

            for (int index = 0; index < shapefileDataReader.DbaseHeader.NumFields; index++)
                sbCreate.Append(String.Format("       [{0}] NVARCHAR(MAX),", shapefileDataReader.DbaseHeader.Fields[index].Name.ToUpper()));

            sbCreate.Append("       GEO GEOMETRY");
            sbCreate.Append(")");

            SqlCommand scCreate = new SqlCommand(sbCreate.ToString(), sqlConnection);
            scCreate.ExecuteNonQuery();
        }

        private void DoEvents()
        {
            DispatcherFrame frame = new DispatcherFrame();
            Dispatcher.CurrentDispatcher.BeginInvoke(DispatcherPriority.Background,
            (SendOrPostCallback)delegate(object arg)
            {
                DispatcherFrame fr = arg as DispatcherFrame;
                fr.Continue = false;
            }, frame);
            Dispatcher.PushFrame(frame);
        }

        private void ExecutarInclusao(SqlCommand sqlCommand, ShapefileDataReader shapefileDataReader)
        {
            for (int index = 0; index < shapefileDataReader.DbaseHeader.NumFields; index++)
                sqlCommand.Parameters[String.Format("@{0}", shapefileDataReader.DbaseHeader.Fields[index].Name.ToUpper())].Value = shapefileDataReader.GetString(index);

            if (shapefileDataReader.Geometry.IsEmpty)
                sqlCommand.Parameters["@GEO"].Value = DBNull.Value;
            else
                sqlCommand.Parameters["@GEO"].Value = shapefileDataReader.Geometry.AsBinary();

            sqlCommand.ExecuteNonQuery();
        }

        private void GetFiles(string path, ref List<string> files)
        {
            foreach (string directory in Directory.GetDirectories(path))
                this.GetFiles(directory, ref files);

            foreach (string file in Directory.GetFiles(path, "*.shp"))
                files.Add(file);
        }

        private string RetornarNomeTabela(string file)
        {
            string nomeTabela = Path.GetFileName(file).Replace(Path.GetExtension(file), String.Empty);

            if ((this.tbPrefixo.Text.Length > 0) && (this.tbPrefixo.Text.Length < nomeTabela.Length) && (nomeTabela.Substring(0, this.tbPrefixo.Text.Length) == this.tbPrefixo.Text))
                nomeTabela = nomeTabela.Substring(this.tbPrefixo.Text.Length);

            return nomeTabela;
        }

        private void btnOk_Click(object sender, RoutedEventArgs e)
        {
            List<string> files = new List<string>();

            this.GetFiles(this.tbDiretorioEntrada.Text, ref files);

            this.pbGeral.Value = 0;
            this.pbGeral.Maximum = files.Count;
            this.pbGeral.Visibility = Visibility.Visible;
            this.DoEvents();

            using (SqlConnection sqlConnection = new SqlConnection(this.tbDatabaseDestino.Text))
            {
                sqlConnection.Open();

                GeometryFactory geometryFactory = new GeometryFactory();
                SqlCommand sqlCommand;

                foreach (string file in files)
                {
                    using (ShapefileDataReader shapefileDataReader = Shapefile.CreateDataReader(file, geometryFactory))
                    {
                        if (shapefileDataReader.Read())
                        {
                            this.pbArquivo.Value = 0;
                            this.pbArquivo.Maximum = shapefileDataReader.RecordCount;
                            this.pbArquivo.Visibility = Visibility.Visible;
                            this.DoEvents();

                            this.CriarTabela(sqlConnection, file, shapefileDataReader);

                            sqlCommand = this.CriarComandoInclusao(sqlConnection, file, shapefileDataReader);

                            do
                            {
                                this.ExecutarInclusao(sqlCommand, shapefileDataReader);

                                this.pbArquivo.Value++;
                                this.DoEvents();
                            }
                            while (shapefileDataReader.Read());

                            this.pbArquivo.Visibility = Visibility.Hidden;
                            this.DoEvents();
                        }

                        shapefileDataReader.Close();
                    }

                    this.pbGeral.Value++;
                    this.DoEvents();
                }

                sqlConnection.Close();
            }

            this.pbGeral.Visibility = Visibility.Hidden;
            this.DoEvents();
        }
    }
}