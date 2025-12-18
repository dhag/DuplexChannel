namespace DuplexChannel
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        async private void button1_Click(object sender, EventArgs e)
        {
            await HagLib.NET.Duplex.Examples.TcpExample.BasicExample();
            //await HagLib.NET.Duplex.Examples.PipeExample.BasicExample();
            //await HagLib.NET.Duplex.Examples.UdpExample.BasicExample();
        }

        async private void button2_Click(object sender, EventArgs e)
        {
            await HagLib.NET.Duplex.Examples.TypedPayloadExample.MixedPayloadExample();
        }
    }
}
