import Link from "next/link"

const page = () => {
  return (
    <div className="pt-5 m-5">
      <div className="text-2xl text-center">Select mode</div>
      <div className="flex gap-3 justify-center pt-4">
        <Link className="p-2 rounded bg-black text-white hover:cursor-pointer" href={"master"}>Master</Link>
        <Link className="p-2 rounded bg-black text-white hover:cursor-pointer" href={"viewer"}>Viewer</Link>
      </div>
    </div>
  )
}

export default page