using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace PersistenceConsumer.Models;

public class NotificationLogInfo
{
    public BigInteger Id { get; set; }
    public BigInteger AnomalyId { get; set; }
    public DateTime SendAt { get; set; }
    public int Attempts { get; set; }
}