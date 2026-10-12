// Stub for local mock — MAIN uses PREP/scripts/ReadOffsetsTopic.java on CDP.
public class ReadOffsetsTopic {
  public static void main(String[] args) {
    System.out.println("{\"partition\":{\"server\":\"" + args[1].split("\\.")[0] + "\"},\"offset\":{\"scn\":\"1\"}}");
  }
}
